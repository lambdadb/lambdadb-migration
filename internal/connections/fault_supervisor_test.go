package connections

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Test-only supervisor: the unchanged worker is a child, so SIGKILL does not
// require ECS Exec, extra capabilities or PID-namespace changes in a real task.
func superviseFaultWorker(cmd *exec.Cmd, signals <-chan os.Signal, poll func() (bool, error), emit func(string, int), interval time.Duration) (int, error) {
	if err := cmd.Start(); err != nil {
		return 1, errors.New("test worker could not start")
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	reaped := false
	defer func() {
		if !reaped {
			_ = cmd.Process.Kill()
			<-done
		}
	}()
	emit("supervisor_worker_started", cmd.Process.Pid)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	killed := false
	for {
		select {
		case err := <-done:
			reaped = true
			code := cmd.ProcessState.ExitCode()
			if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() {
				code = 128 + int(status.Signal())
			}
			emit("supervisor_worker_exited", code)
			if err != nil && code == 0 {
				return 1, errors.New("test worker wait failed")
			}
			return code, nil
		case sig := <-signals:
			if err := cmd.Process.Signal(sig); err != nil && !errors.Is(err, os.ErrProcessDone) {
				return 1, errors.New("test worker signal failed")
			}
		case <-ticker.C:
			if killed {
				continue
			}
			kill, err := poll()
			if err != nil {
				continue
			} // An unavailable test controller never authorizes a kill.
			if kill {
				if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
					return 1, errors.New("test worker kill failed")
				}
				killed = true
				emit("supervisor_worker_killed", cmd.Process.Pid)
			}
		}
	}
}

func taskMetadataOrigin(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	ip := net.ParseIP(u.Hostname()).To4()
	return u.Scheme == "http" && len(ip) == 4 && ip[0] == 169 && ip[1] == 254 && ip[2] == 170 && u.User == nil && u.RawQuery == "" && u.Fragment == ""
}

func TestConnectionFaultSupervisor(t *testing.T) {
	origin := os.Getenv("CONNECTION_TEST_FAULT_CONTROL_URL")
	if origin == "" {
		t.Skip("explicit private fault supervisor required")
	}
	u, err := url.Parse(origin)
	if err != nil || u.Scheme != "https" || !net.ParseIP(u.Hostname()).IsPrivate() || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" {
		t.Fatal("private verified fault control origin required")
	}
	metadata := os.Getenv("ECS_CONTAINER_METADATA_URI_V4")
	if !taskMetadataOrigin(metadata) {
		t.Fatal("task-local ECS metadata required")
	}
	client := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Get(strings.TrimRight(metadata, "/") + "/task")
	if err != nil {
		t.Fatal("task identity unavailable")
	}
	var identity struct{ TaskARN string }
	err = json.NewDecoder(io.LimitReader(response.Body, 65536)).Decode(&identity)
	_ = response.Body.Close()
	if response.StatusCode != 200 || err != nil || !strings.HasPrefix(identity.TaskARN, "arn:aws:ecs:"+os.Getenv("AWS_REGION")+":") {
		t.Fatal("invalid task identity")
	}
	pem, err := os.ReadFile("/state/regional-ca.pem")
	if err != nil {
		t.Fatal("test CA unavailable")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		t.Fatal("test CA invalid")
	}
	client.Transport = &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	credential := os.Getenv("CONNECTION_WORKER_CREDENTIAL")
	if len(credential) < 32 {
		t.Fatal("scoped test credential unavailable")
	}
	var args []string
	if json.Unmarshal([]byte(os.Getenv("CONNECTION_TEST_WORKER_ARGS")), &args) != nil || len(args) == 0 {
		t.Fatal("explicit worker arguments required")
	}
	workerPID := 0
	emit := func(event string, value int) {
		if event == "supervisor_worker_started" {
			workerPID = value
		}
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"event": event, "value": value, "task_arn": identity.TaskARN, "at": time.Now().UTC().Format(time.RFC3339Nano)})
	}
	storage := &nfsFaultProbe{enabled: os.Getenv("CONNECTION_TEST_NFS_FAULTS") == "1", emit: emit}
	poll := func() (bool, error) {
		raw, _ := json.Marshal(map[string]string{"task_arn": identity.TaskARN})
		req, _ := http.NewRequest("POST", origin+"/test/kill", bytes.NewReader(raw))
		req.Header.Set("Authorization", "Bearer "+credential)
		res, err := client.Do(req)
		if err != nil {
			return false, err
		}
		defer res.Body.Close()
		var command struct {
			TaskARN string `json:"task_arn"`
			Kill    bool   `json:"kill"`
			NFS     string `json:"nfs"`
		}
		if res.StatusCode != 200 || json.NewDecoder(io.LimitReader(res.Body, 4096)).Decode(&command) != nil || command.TaskARN != identity.TaskARN {
			return false, errors.New("invalid test kill response")
		}
		if err := storage.update(command.NFS, workerPID); err != nil {
			return false, err
		}
		return command.Kill, nil
	}
	cmd := exec.Command("/usr/local/bin/connection-worker", args...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	termination := make(chan os.Signal, 2)
	signal.Notify(termination, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(termination)
	code, err := superviseFaultWorker(cmd, termination, poll, emit, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		os.Exit(code)
	}
}

func TestFaultSupervisorForwardsTerminationAndProvesSIGKILL(t *testing.T) {
	for _, force := range []bool{false, true} {
		t.Run(fmt.Sprint(force), func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestFaultSupervisorChild$")
			cmd.Env = append(os.Environ(), "CONNECTION_FAULT_CHILD=1")
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			ready := make(chan struct{})
			go func() { scanner := bufio.NewScanner(stdout); scanner.Scan(); close(ready) }()
			termination := make(chan os.Signal, 1)
			if !force {
				go func() { <-ready; termination <- syscall.SIGTERM }()
			}
			killed := false
			code, err := superviseFaultWorker(cmd, termination, func() (bool, error) { <-ready; return force, nil }, func(event string, _ int) {
				if event == "supervisor_worker_killed" {
					killed = true
				}
			}, 10*time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			want := 0
			if force {
				want = 137
			}
			if code != want || killed != force {
				t.Fatalf("code=%d killed=%v", code, killed)
			}
		})
	}
}
func TestFaultSupervisorChild(t *testing.T) {
	if os.Getenv("CONNECTION_FAULT_CHILD") != "1" {
		t.Skip("test subprocess only")
	}
	termination := make(chan os.Signal, 1)
	signal.Notify(termination, syscall.SIGTERM)
	fmt.Println("ready")
	<-termination
	os.Exit(0)
}
func TestFaultSupervisorMetadataCannotUseExternalOrigins(t *testing.T) {
	for _, raw := range []string{"https://public.example", "http://127.0.0.1", "http://169.254.170.2?key=secret", "http://user@169.254.170.2"} {
		if taskMetadataOrigin(raw) {
			t.Fatal("non-task metadata accepted")
		}
	}
	if !taskMetadataOrigin("http://169.254.170.2/v4/task-token") {
		t.Fatal("ECS task metadata refused")
	}
}
