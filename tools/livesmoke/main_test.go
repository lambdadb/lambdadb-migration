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

func smokeEnv() []string {
	return []string{
		"LAMBDADB_BASE_URL=https://test.example",
		"LAMBDADB_PROJECT_NAME=migration-ci",
		"LAMBDADB_PROJECT_API_KEY= synthetic%test-key\n",
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

// fakeGo writes goOutput for go test and returns goErr. It runs no other command.
func fakeGo(t *testing.T, goOutput string, goErr error, calls *[]string) commandRunner {
	return func(name string, args, _ []string, stdout, _ io.Writer) error {
		*calls = append(*calls, name)
		if name != "go" {
			t.Fatalf("unexpected command %s", name)
		}
		if slices.ContainsFunc(args, func(arg string) bool { return strings.Contains(arg, "synthetic") }) {
			t.Error("go test arguments contain the key")
		}
		io.WriteString(stdout, goOutput)
		return goErr
	}
}

func TestRunPassesTheKeyOnlyInTheGoTestEnvironment(t *testing.T) {
	environ := smokeEnv()
	before := slices.Clone(environ)
	var childEnv, args []string
	var calls []string
	commands := fakeGo(t, passingRun, nil, &calls)
	var stdout bytes.Buffer
	err := run(environ, func(name string, a, env []string, out, errOut io.Writer) error {
		childEnv, args = env, a
		return commands(name, a, env, out, errOut)
	}, &stdout, io.Discard)
	if err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if !reflect.DeepEqual(calls, []string{"go"}) {
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
	if n := len(slices.DeleteFunc(slices.Clone(childEnv), func(e string) bool { return !strings.HasPrefix(e, "LAMBDADB_PROJECT_API_KEY=") })); n != 1 {
		t.Errorf("child env has %d project keys, want 1", n)
	}
	if !reflect.DeepEqual(environ, before) {
		t.Error("run() changed the parent environment")
	}
	// Actions masks the Environment secret, so the runner prints no mask command or key.
	if output := stdout.String(); output != "=== RUN   TestFixtureSourcesToLambdaDBLive\n" {
		t.Fatalf("output = %q, want only the decoded test output", output)
	}
}

func TestRunRejectsMissingConfigurationAndKeysBeforeTesting(t *testing.T) {
	for _, name := range requiredConfig {
		absent := slices.DeleteFunc(smokeEnv(), func(entry string) bool { return strings.HasPrefix(entry, name+"=") })
		for _, env := range [][]string{absent, append(slices.Clone(absent), name+"="), append(slices.Clone(absent), name+"= \n")} {
			var calls []string
			err := run(env, fakeGo(t, passingRun, nil, &calls), io.Discard, io.Discard)
			if err == nil || err.Error() != "missing smoke configuration: "+name || len(calls) != 0 {
				t.Errorf("%s: run() error = %v, commands = %v", name, err, calls)
			}
		}
	}

	for _, key := range []string{"key\nother-key", "key\rother-key"} {
		env := append(slices.DeleteFunc(smokeEnv(), func(entry string) bool { return strings.HasPrefix(entry, "LAMBDADB_PROJECT_API_KEY=") }),
			"LAMBDADB_PROJECT_API_KEY="+key)
		var calls []string
		var stdout bytes.Buffer
		err := run(env, fakeGo(t, passingRun, nil, &calls), &stdout, io.Discard)
		if err == nil || err.Error() != "expected a single project API key" || len(calls) != 0 || stdout.Len() != 0 {
			t.Errorf("key %q: run() error = %v, commands = %v, output = %q", key, err, calls, stdout.String())
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
			err := run(smokeEnv(), fakeGo(t, tc.output, tc.err, &calls), io.Discard, io.Discard)
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
