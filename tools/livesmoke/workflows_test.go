package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type workflow struct {
	On   map[string]any `yaml:"on"`
	Jobs map[string]struct {
		If          string            `yaml:"if"`
		Needs       any               `yaml:"needs"`
		Environment string            `yaml:"environment"`
		Uses        string            `yaml:"uses"`
		With        map[string]string `yaml:"with"`
		Steps       []struct {
			Name string `yaml:"name"`
			Run  string `yaml:"run"`
		} `yaml:"steps"`
	} `yaml:"jobs"`
}

func readWorkflow(t *testing.T, name string) (workflow, string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", name))
	if err != nil {
		t.Fatal(err)
	}
	var wf workflow
	if err := yaml.Unmarshal(data, &wf); err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	return wf, string(data)
}

// Each trigger selects one environment: main pushes test dev; release
// verification and tag releases test production before anything is published.
func TestWorkflowsSelectTheLiveSmokeEnvironment(t *testing.T) {
	const smoke = "./.github/workflows/live-smoke.yml"
	callers := map[string]string{}
	for _, name := range []string{"ci.yml", "release.yml", "release-verification.yml"} {
		wf, _ := readWorkflow(t, name)
		for jobName, job := range wf.Jobs {
			if job.Uses == smoke {
				callers[name+":"+jobName] = job.With["environment"]
			}
		}
	}
	want := map[string]string{
		"ci.yml:live-smoke":                   "dev",
		"release.yml:live-smoke":              "prod",
		"release-verification.yml:live-smoke": "prod",
	}
	if len(callers) != len(want) {
		t.Fatalf("live smoke callers = %v, want %v", callers, want)
	}
	for caller, environment := range want {
		if callers[caller] != environment {
			t.Errorf("%s environment = %q, want %q", caller, callers[caller], environment)
		}
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
	if got := verification.Jobs["live-smoke"].With["ref"]; got != "${{ needs.resolve.outputs.sha }}" {
		t.Errorf("release-verification.yml live-smoke ref = %q, want the resolved main commit", got)
	}
}

func TestLiveSmokeWorkflowMapsEnvironmentToVariablePrefix(t *testing.T) {
	wf, source := readWorkflow(t, "live-smoke.yml")
	var script string
	for _, step := range wf.Jobs["target"].Steps {
		if step.Name == "Select the LambdaDB environment" {
			script = step.Run
		}
	}
	if script == "" {
		t.Fatal("live-smoke.yml has no environment selection step")
	}
	for environment, want := range map[string]string{"dev": "LAMBDADB_DEV_", "prod": "LAMBDADB_PROD_", "staging": "", "": "", "DEV": ""} {
		output := filepath.Join(t.TempDir(), "output")
		cmd := exec.Command("bash", "-e", "-c", script)
		cmd.Env = append(os.Environ(), "SMOKE_ENVIRONMENT="+environment, "GITHUB_OUTPUT="+output)
		err := cmd.Run()
		got, _ := os.ReadFile(output)
		if want == "" {
			if err == nil || len(got) != 0 {
				t.Errorf("environment %q: err = %v, output = %q, want failure", environment, err, got)
			}
		} else if err != nil || string(got) != "prefix="+want+"\nenvironment="+environment+"\n" {
			t.Errorf("environment %q: err = %v, output = %q, want prefix %s", environment, err, got, want)
		}
	}

	// Every Variable lookup uses the selected prefix, never a fixed environment.
	lookups := strings.Count(source, "vars[")
	if lookups != 2 || strings.Count(source, "vars[format('{0}") != lookups || strings.Count(source, "needs.target.outputs.prefix)]") != lookups {
		t.Errorf("live-smoke.yml must read its 2 Variables through the selected prefix")
	}
}

// The smoke key comes only from the GitHub Environment the selection job
// picks; no AWS step, ARN, or OIDC token remains.
func TestLiveSmokeReadsTheKeyFromTheSelectedEnvironment(t *testing.T) {
	wf, source := readWorkflow(t, "live-smoke.yml")
	smoke := wf.Jobs["smoke"]
	if smoke.Needs != "target" || smoke.Environment != "${{ needs.target.outputs.environment }}" {
		t.Errorf("smoke job needs = %v, environment = %q, want the target job's environment", smoke.Needs, smoke.Environment)
	}
	if wf.Jobs["target"].Environment != "" {
		t.Error("the selection job must not run in an Environment")
	}
	if strings.Count(source, "secrets.") != 1 || !strings.Contains(source, "LAMBDADB_PROJECT_API_KEY: ${{ secrets.LAMBDADB_PROJECT_API_KEY }}") {
		t.Error("live-smoke.yml must read only LAMBDADB_PROJECT_API_KEY from secrets")
	}
	for _, name := range []string{"ci.yml", "release.yml", "release-verification.yml"} {
		// Without inherited secrets the called smoke job sees an empty key.
		if _, text := readWorkflow(t, name); !regexp.MustCompile(`uses: \./\.github/workflows/live-smoke\.yml\n(?: {4}.*\n)*? {4}secrets: inherit\n`).MatchString(text) {
			t.Errorf("%s must call live-smoke.yml with secrets: inherit", name)
		}
	}
	for _, name := range []string{"ci.yml", "release.yml", "release-verification.yml", "live-smoke.yml"} {
		_, text := readWorkflow(t, name)
		for _, removed := range []string{"configure-aws-credentials", "SMOKE_AWS_ROLE_ARN", "SMOKE_SECRET_ARN", "id-token"} {
			if strings.Contains(text, removed) {
				t.Errorf("%s still contains %s", name, removed)
			}
		}
	}
}
