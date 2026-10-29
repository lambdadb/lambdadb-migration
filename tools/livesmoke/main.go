// Command livesmoke runs the fixture live suite against a deployed LambdaDB
// environment. It reads the project API key from AWS Secrets Manager, masks it
// in GitHub Actions logs, and passes it only in the go test child environment.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

const (
	liveFlag = "LAMBDADB_MIGRATION_RUN_FIXTURE_LIVE_E2E"
	liveTest = "TestFixtureSourcesToLambdaDBLive"
)

var (
	requiredConfig = []string{"LAMBDADB_BASE_URL", "LAMBDADB_PROJECT_NAME", "LAMBDADB_SMOKE_SECRET_ARN"}
	goTestArgs     = []string{"test", "./integration_tests", "-run", "^" + liveTest + "$", "-count=1", "-timeout=30m", "-json"}
)

// commandRunner runs name with args and environment env.
type commandRunner func(name string, args, env []string, stdout, stderr io.Writer) error

func main() {
	if err := run(os.Environ(), execCommand, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func execCommand(name string, args, env []string, stdout, stderr io.Writer) error {
	cmd := exec.Command(name, args...)
	cmd.Env, cmd.Stdout, cmd.Stderr = env, stdout, stderr
	return cmd.Run()
}

func run(environ []string, execute commandRunner, stdout, stderr io.Writer) error {
	config := map[string]string{}
	for _, entry := range environ {
		if name, value, ok := strings.Cut(entry, "="); ok {
			config[name] = value
		}
	}
	for _, name := range requiredConfig {
		if strings.TrimSpace(config[name]) == "" {
			return fmt.Errorf("missing smoke configuration: %s", name)
		}
	}

	var secret bytes.Buffer
	if err := execute("aws", []string{"secretsmanager", "get-secret-value", "--region", "ap-northeast-2",
		"--secret-id", config["LAMBDADB_SMOKE_SECRET_ARN"], "--query", "SecretString", "--output", "text"},
		environ, &secret, io.Discard); err != nil {
		// Do not include AWS command output in errors.
		return errors.New("unable to read the smoke project key from Secrets Manager")
	}
	key := strings.TrimSpace(secret.String())
	if key == "" || key == "None" || strings.ContainsAny(key, "\r\n") {
		return errors.New("expected a single project API key in SecretString")
	}
	// The workflow command hides the key in GitHub Actions logs. Elsewhere it
	// would print the key, so local runs skip it.
	if config["GITHUB_ACTIONS"] == "true" {
		fmt.Fprintf(stdout, "::add-mask::%s\n", strings.ReplaceAll(key, "%", "%25"))
	}

	// Keep the key out of files, command arguments, and the parent environment.
	env := make([]string, 0, len(environ)+2)
	for _, entry := range environ {
		name, _, _ := strings.Cut(entry, "=")
		if name != "LAMBDADB_PROJECT_API_KEY" && name != liveFlag {
			env = append(env, entry)
		}
	}
	env = append(env, "LAMBDADB_PROJECT_API_KEY="+key, liveFlag+"=1")

	events := &testEvents{out: stdout}
	err := execute("go", goTestArgs, env, events, stderr)
	events.Flush()
	if err != nil {
		return errors.New("deployed live smoke failed")
	}
	return events.Verify()
}

// testEvents prints go test -json output and records which tests passed or
// were skipped, so a skipped or unmatched live test cannot count as a pass.
type testEvents struct {
	out     io.Writer
	pending []byte
	passed  map[string]bool
	skipped []string
}

func (e *testEvents) Write(data []byte) (int, error) {
	e.pending = append(e.pending, data...)
	for {
		line, rest, ok := bytes.Cut(e.pending, []byte("\n"))
		if !ok {
			return len(data), nil
		}
		e.handle(line)
		e.pending = rest
	}
}

func (e *testEvents) Flush() {
	if len(e.pending) > 0 {
		e.handle(e.pending)
		e.pending = nil
	}
}

func (e *testEvents) handle(line []byte) {
	var event struct {
		Action string
		Test   string
		Output string
	}
	if err := json.Unmarshal(line, &event); err != nil {
		fmt.Fprintf(e.out, "%s\n", line)
		return
	}
	switch event.Action {
	case "output":
		io.WriteString(e.out, event.Output)
	case "pass":
		if e.passed == nil {
			e.passed = map[string]bool{}
		}
		e.passed[event.Test] = true
	case "skip":
		if event.Test == "" {
			event.Test = "the test package"
		}
		e.skipped = append(e.skipped, event.Test)
	}
}

func (e *testEvents) Verify() error {
	if len(e.skipped) > 0 {
		return fmt.Errorf("deployed live smoke skipped %s", strings.Join(e.skipped, ", "))
	}
	if !e.passed[liveTest] {
		return fmt.Errorf("deployed live smoke did not run %s", liveTest)
	}
	return nil
}
