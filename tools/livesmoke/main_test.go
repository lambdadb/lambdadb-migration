package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strings"
	"testing"
)

const secretARN = "arn:aws:secretsmanager:ap-northeast-2:123456789012:secret:smoke-key"

func smokeEnv() []string {
	return []string{
		"LAMBDADB_BASE_URL=https://test.example",
		"LAMBDADB_PROJECT_NAME=migration-ci",
		"LAMBDADB_SMOKE_SECRET_ARN=" + secretARN,
		"LAMBDADB_PROJECT_API_KEY=stale-key",
		"PATH=/bin",
		"GITHUB_ACTIONS=true",
	}
}

func goTestEvents(lines ...string) string {
	return strings.Join(lines, "\n") + "\n"
}

var passingRun = goTestEvents(
	`{"Action":"run","Test":"TestFixtureSourcesToLambdaDBLive"}`,
	`{"Action":"output","Test":"TestFixtureSourcesToLambdaDBLive","Output":"=== RUN   TestFixtureSourcesToLambdaDBLive\n"}`,
	`{"Action":"pass","Test":"TestFixtureSourcesToLambdaDBLive/qdrant_unnamed_dense_upsert"}`,
	`{"Action":"pass","Test":"TestFixtureSourcesToLambdaDBLive"}`,
	`{"Action":"pass"}`,
)

// fakeCommands returns the secret for aws and goOutput for go, then the result.
func fakeCommands(t *testing.T, secret, goOutput string, goErr error, calls *[]string) commandRunner {
	return func(name string, args, env []string, stdout, stderr io.Writer) error {
		*calls = append(*calls, name)
		switch name {
		case "aws":
			want := []string{"secretsmanager", "get-secret-value", "--region", "ap-northeast-2",
				"--secret-id", secretARN, "--query", "SecretString", "--output", "text"}
			if !reflect.DeepEqual(args, want) {
				t.Errorf("aws args = %q, want %q", args, want)
			}
			if stderr != io.Discard {
				t.Error("aws stderr must be discarded")
			}
			io.WriteString(stdout, secret)
			return nil
		case "go":
			for _, value := range secretValues(secret) {
				if slices.ContainsFunc(args, func(arg string) bool { return strings.Contains(arg, value) }) {
					t.Error("go test arguments contain the key")
				}
			}
			io.WriteString(stdout, goOutput)
			return goErr
		}
		t.Fatalf("unexpected command %s", name)
		return nil
	}
}

func secretValues(secret string) []string {
	if key := strings.TrimSpace(secret); key != "" {
		return []string{key}
	}
	return nil
}

func TestRunPassesMaskedKeyOnlyToTheGoTestEnvironment(t *testing.T) {
	environ := smokeEnv()
	before := slices.Clone(environ)
	var childEnv, args []string
	var calls []string
	commands := fakeCommands(t, "synthetic%test-key\n", passingRun, nil, &calls)
	var stdout bytes.Buffer
	err := run(environ, func(name string, a, env []string, out, errOut io.Writer) error {
		if name == "go" {
			childEnv, args = env, a
		}
		return commands(name, a, env, out, errOut)
	}, &stdout, io.Discard)
	if err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if !reflect.DeepEqual(calls, []string{"aws", "go"}) {
		t.Fatalf("commands = %v", calls)
	}
	wantArgs := []string{"test", "./integration_tests", "-run", "^TestFixtureSourcesToLambdaDBLive$", "-count=1", "-timeout=30m", "-json"}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("go args = %q, want %q", args, wantArgs)
	}
	for _, entry := range []string{"LAMBDADB_PROJECT_API_KEY=synthetic%test-key", liveFlag + "=1", "PATH=/bin", "LAMBDADB_BASE_URL=https://test.example"} {
		if !slices.Contains(childEnv, entry) {
			t.Errorf("child env missing %q", entry)
		}
	}
	if slices.Contains(childEnv, "LAMBDADB_PROJECT_API_KEY=stale-key") {
		t.Error("child env kept the inherited project key")
	}
	if !reflect.DeepEqual(environ, before) {
		t.Error("run() changed the parent environment")
	}
	output := stdout.String()
	if !strings.HasPrefix(output, "::add-mask::synthetic%25test-key\n") {
		t.Fatalf("output = %q, want the mask command before test output", output)
	}
	if strings.Count(output, "synthetic%test-key") != 0 || !strings.Contains(output, "=== RUN   TestFixtureSourcesToLambdaDBLive\n") {
		t.Fatalf("output = %q, want only the masked key and decoded test output", output)
	}
}

func TestRunDoesNotPrintTheKeyOutsideGitHubActions(t *testing.T) {
	env := slices.DeleteFunc(smokeEnv(), func(entry string) bool { return strings.HasPrefix(entry, "GITHUB_ACTIONS=") })
	var calls []string
	var stdout bytes.Buffer
	if err := run(env, fakeCommands(t, "synthetic-key\n", passingRun, nil, &calls), &stdout, io.Discard); err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if strings.Contains(stdout.String(), "synthetic-key") || strings.Contains(stdout.String(), "::add-mask::") {
		t.Fatalf("output = %q, want no key or mask command", stdout.String())
	}
}

func TestRunRejectsMissingConfigurationAndKeysBeforeTesting(t *testing.T) {
	for _, name := range requiredConfig {
		absent := slices.DeleteFunc(smokeEnv(), func(entry string) bool { return strings.HasPrefix(entry, name+"=") })
		for _, env := range [][]string{absent, append(slices.Clone(absent), name+"= ")} {
			var calls []string
			err := run(env, fakeCommands(t, "key", passingRun, nil, &calls), io.Discard, io.Discard)
			if err == nil || err.Error() != "missing smoke configuration: "+name || len(calls) != 0 {
				t.Errorf("%s: run() error = %v, commands = %v", name, err, calls)
			}
		}
	}

	failingAWS := func(name string, _, _ []string, _, stderr io.Writer) error {
		if name != "aws" {
			t.Fatal("must not start the smoke tests")
		}
		io.WriteString(stderr, "sensitive output")
		return errors.New("sensitive output")
	}
	err := run(smokeEnv(), failingAWS, io.Discard, io.Discard)
	if err == nil || err.Error() != "unable to read the smoke project key from Secrets Manager" {
		t.Fatalf("run() error = %v", err)
	}

	for _, secret := range []string{"", "None\n", "key\nother-key\n"} {
		var calls []string
		var stdout bytes.Buffer
		err := run(smokeEnv(), fakeCommands(t, secret, passingRun, nil, &calls), &stdout, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "single project API key") || !reflect.DeepEqual(calls, []string{"aws"}) || stdout.Len() != 0 {
			t.Errorf("secret %q: run() error = %v, commands = %v, output = %q", secret, err, calls, stdout.String())
		}
	}
}

func TestRunFailsUnlessTheLiveTestRanAndPassed(t *testing.T) {
	cases := map[string]struct {
		output string
		err    error
		want   string
	}{
		"test failure": {
			output: goTestEvents(`{"Action":"fail","Test":"TestFixtureSourcesToLambdaDBLive"}`),
			err:    errors.New("exit status 1"),
			want:   "deployed live smoke failed",
		},
		"skipped test": {
			output: goTestEvents(`{"Action":"skip","Test":"TestFixtureSourcesToLambdaDBLive"}`, `{"Action":"pass"}`),
			want:   "deployed live smoke skipped TestFixtureSourcesToLambdaDBLive",
		},
		"skipped case": {
			output: passingRun + goTestEvents(`{"Action":"skip","Test":"TestFixtureSourcesToLambdaDBLive/pinecone_dense_upsert"}`),
			want:   "deployed live smoke skipped TestFixtureSourcesToLambdaDBLive/pinecone_dense_upsert",
		},
		"no matching test": {
			output: goTestEvents(`{"Action":"output","Output":"testing: warning: no tests to run\n"}`, `{"Action":"pass"}`),
			want:   "deployed live smoke did not run TestFixtureSourcesToLambdaDBLive",
		},
		"unterminated final event": {
			output: strings.TrimSuffix(goTestEvents(`{"Action":"skip","Test":"TestFixtureSourcesToLambdaDBLive"}`), "\n"),
			want:   "deployed live smoke skipped TestFixtureSourcesToLambdaDBLive",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var calls []string
			err := run(smokeEnv(), fakeCommands(t, "synthetic-key", tc.output, tc.err, &calls), io.Discard, io.Discard)
			if err == nil || err.Error() != tc.want {
				t.Fatalf("run() error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestEventsHandleSplitWrites(t *testing.T) {
	var out bytes.Buffer
	events := &testEvents{out: &out}
	for _, chunk := range []string{`{"Action":"output","Output":"he`, `llo\n"}` + "\n" + `{"Action":"pa`, `ss","Test":"` + liveTest + `"}` + "\n"} {
		fmt.Fprint(events, chunk)
	}
	events.Flush()
	if out.String() != "hello\n" || events.Verify() != nil {
		t.Fatalf("output = %q, verify = %v", out.String(), events.Verify())
	}
}
