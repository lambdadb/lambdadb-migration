package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type workflowStep struct {
	Name string            `yaml:"name"`
	Uses string            `yaml:"uses"`
	Run  string            `yaml:"run"`
	With map[string]string `yaml:"with"`
	Env  map[string]string `yaml:"env"`
}

type workflow struct {
	On   map[string]any `yaml:"on"`
	Jobs map[string]struct {
		If          string         `yaml:"if"`
		Needs       any            `yaml:"needs"`
		Environment string         `yaml:"environment"`
		Uses        string         `yaml:"uses"`
		Steps       []workflowStep `yaml:"steps"`
	} `yaml:"jobs"`
}

const liveSmokeAction = "./.github/actions/live-smoke"

func readFile(t *testing.T, path ...string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(append([]string{"..", ".."}, path...)...))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func readWorkflow(t *testing.T, name string) (workflow, string) {
	t.Helper()
	data := readFile(t, ".github", "workflows", name)
	var wf workflow
	if err := yaml.Unmarshal(data, &wf); err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	return wf, string(data)
}

// smokeStep returns the job's single step that runs the live smoke action.
func smokeStep(t *testing.T, wf workflow, jobName string) workflowStep {
	t.Helper()
	var found []workflowStep
	for _, step := range wf.Jobs[jobName].Steps {
		if step.Uses == liveSmokeAction {
			found = append(found, step)
		}
	}
	if len(found) != 1 {
		t.Fatalf("job %s runs the live smoke action %d times, want 1", jobName, len(found))
	}
	return found[0]
}

// Each trigger selects one environment: main pushes test dev; release
// verification and tag releases test production before anything is published.
// The job that runs the smoke declares the GitHub Environment itself and reads
// only that environment's Variables, so it never falls back to the other one.
func TestWorkflowsSelectTheLiveSmokeEnvironment(t *testing.T) {
	want := map[string]string{
		"ci.yml":                   "dev",
		"release.yml":              "prod",
		"release-verification.yml": "prod",
	}
	callers := 0
	for _, name := range []string{"ci.yml", "release.yml", "release-verification.yml"} {
		wf, _ := readWorkflow(t, name)
		for jobName, job := range wf.Jobs {
			for _, step := range job.Steps {
				if step.Uses == liveSmokeAction {
					callers++
					if jobName != "live-smoke" {
						t.Errorf("%s runs the live smoke in job %s, want live-smoke", name, jobName)
					}
				}
			}
		}
		environment := want[name]
		job := wf.Jobs["live-smoke"]
		if job.Environment != environment {
			t.Errorf("%s live-smoke environment = %q, want %q", name, job.Environment, environment)
		}
		prefix := "LAMBDADB_" + strings.ToUpper(environment) + "_"
		step := smokeStep(t, wf, "live-smoke")
		wantWith := map[string]string{
			"base-url":        "${{ vars." + prefix + "BASE_URL }}",
			"project-name":    "${{ vars." + prefix + "PROJECT_NAME }}",
			"project-api-key": "${{ secrets.LAMBDADB_PROJECT_API_KEY }}",
		}
		if len(step.With) != len(wantWith) {
			t.Errorf("%s live smoke inputs = %v, want %v", name, step.With, wantWith)
		}
		for input, value := range wantWith {
			if step.With[input] != value {
				t.Errorf("%s live smoke %s = %q, want %q", name, input, step.With[input], value)
			}
		}
	}
	if callers != len(want) {
		t.Errorf("live smoke action runs %d times across workflows, want %d", callers, len(want))
	}

	ci, _ := readWorkflow(t, "ci.yml")
	if got := ci.Jobs["live-smoke"].If; got != "github.event_name == 'push' && github.ref == 'refs/heads/main'" {
		t.Errorf("ci.yml live-smoke if = %q, want main pushes only", got)
	}
	release, _ := readWorkflow(t, "release.yml")
	if got := release.Jobs["goreleaser"].Needs; got != "live-smoke" {
		t.Errorf("release.yml goreleaser needs = %v, want live-smoke", got)
	}
	verification, _ := readWorkflow(t, "release-verification.yml")
	if _, ok := verification.On["workflow_dispatch"]; !ok || len(verification.On) != 1 {
		t.Errorf("release-verification.yml triggers = %v, want workflow_dispatch only", verification.On)
	}
	checkout := verification.Jobs["live-smoke"].Steps[0]
	if !strings.HasPrefix(checkout.Uses, "actions/checkout@") || checkout.With["ref"] != "${{ needs.resolve.outputs.sha }}" {
		t.Errorf("release-verification.yml live-smoke must first check out the resolved main commit, got %+v", checkout)
	}
}

// The smoke key comes only from the calling job's GitHub Environment; the
// action receives it as an input and passes it to the runner's environment. No
// AWS step, ARN, OIDC token, inherited secrets, or reusable workflow remains.
func TestLiveSmokeReadsTheKeyFromTheJobEnvironment(t *testing.T) {
	var action struct {
		Runs struct {
			Using string         `yaml:"using"`
			Steps []workflowStep `yaml:"steps"`
		} `yaml:"runs"`
	}
	source := readFile(t, ".github", "actions", "live-smoke", "action.yml")
	if err := yaml.Unmarshal(source, &action); err != nil {
		t.Fatal(err)
	}
	if action.Runs.Using != "composite" {
		t.Fatalf("live smoke action runs using %q, want composite", action.Runs.Using)
	}
	var runner workflowStep
	for _, step := range action.Runs.Steps {
		if strings.Contains(step.Run, "go run ./tools/livesmoke") {
			runner = step
		}
	}
	wantEnv := map[string]string{
		"LAMBDADB_BASE_URL":        "${{ inputs.base-url }}",
		"LAMBDADB_PROJECT_NAME":    "${{ inputs.project-name }}",
		"LAMBDADB_PROJECT_API_KEY": "${{ inputs.project-api-key }}",
	}
	if len(runner.Env) != len(wantEnv) {
		t.Errorf("live smoke runner env = %v, want %v", runner.Env, wantEnv)
	}
	for name, value := range wantEnv {
		if runner.Env[name] != value {
			t.Errorf("live smoke runner %s = %q, want %q", name, runner.Env[name], value)
		}
	}
	if strings.Contains(string(source), "secrets.") || strings.Contains(string(source), "vars.") {
		t.Error("the live smoke action must receive its configuration only through inputs")
	}

	if _, err := os.Stat(filepath.Join("..", "..", ".github", "workflows", "live-smoke.yml")); !os.IsNotExist(err) {
		t.Errorf("the reusable live-smoke.yml workflow must not exist (stat error %v)", err)
	}
	for _, name := range []string{"ci.yml", "release.yml", "release-verification.yml"} {
		_, text := readWorkflow(t, name)
		if strings.Count(text, "secrets.LAMBDADB_PROJECT_API_KEY") != 1 {
			t.Errorf("%s must read LAMBDADB_PROJECT_API_KEY exactly once", name)
		}
		for _, removed := range []string{"configure-aws-credentials", "SMOKE_AWS_ROLE_ARN", "SMOKE_SECRET_ARN", "id-token", "secrets: inherit", "workflow_call", "live-smoke.yml"} {
			if strings.Contains(text, removed) {
				t.Errorf("%s still contains %s", name, removed)
			}
		}
	}
}
