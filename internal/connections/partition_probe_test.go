package connections

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const partitionWindow = 5 * time.Minute

func partitionEvent(event string, fields map[string]any) {
	if fields == nil {
		fields = map[string]any{}
	}
	fields["event"], fields["at"] = event, time.Now().UTC().Format(time.RFC3339Nano)
	_ = json.NewEncoder(os.Stdout).Encode(fields)
}

// The agent URI is provided by ECS. Never forward its requests through a proxy
// or redirect, or permit an arbitrary public endpoint in this test-only helper.
func partitionFault(client *http.Client, origin, action string) error {
	u, err := url.Parse(origin)
	if err != nil {
		return errors.New("invalid ECS agent URI")
	}
	ip := net.ParseIP(u.Hostname()).To4()
	if u.Scheme != "http" || len(ip) != 4 || ip[0] != 169 || ip[1] != 254 || ip[2] != 170 || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (action != "start" && action != "stop") {
		return errors.New("ECS fault requests require a task-local agent and known action")
	}
	body := []byte(`{"Port":2049,"Protocol":"tcp","TrafficType":"egress"}`)
	req, err := http.NewRequestWithContext(context.Background(), "POST", strings.TrimRight(origin, "/")+"/fault/v1/network-blackhole-port/"+action, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := client.Do(req)
	if err != nil {
		return errors.New("ECS fault endpoint request failed")
	}
	defer response.Body.Close()
	var result struct{ Status string }
	want := "running"
	if action == "stop" {
		want = "stopped"
	}
	if response.StatusCode != 200 || json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&result) != nil || result.Status != want {
		return errors.New("ECS fault endpoint did not confirm requested state")
	}
	return nil
}

// Opt-in synthetic experiment. It tests journal ownership, not the entire worker
// loop, PostgreSQL continuity or remote target acceptance. No target is contacted.
func TestEFSPartitionProbe(t *testing.T) {
	dir, phase := os.Getenv("CONNECTION_VOLUME_PROBE_DIR"), os.Getenv("CONNECTION_VOLUME_PROBE_PHASE")
	if dir == "" && phase == "" {
		t.Skip("explicit disposable partition probe required")
	}
	if !filepath.IsAbs(dir) || (phase != "owner" && phase != "takeover" && phase != "verify-acquired" && phase != "verify-retained") {
		t.Fatal("absolute disposable directory and known partition phase required")
	}
	path := filepath.Join(dir, "connection.db")
	if phase == "owner" {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal("partition directory must be fresh", err)
		}
	}
	if phase == "takeover" {
		deadline := time.Now().Add(3 * time.Minute)
		for time.Now().Before(deadline) {
			if info, err := os.Stat(path); err != nil || info.Size() == 0 {
				t.Fatal("original journal is not visible")
			}
			j, err := OpenJournal(path, "partition-probe", false)
			if err != nil {
				time.Sleep(2 * time.Second)
				continue
			}
			defer j.Close()
			partitionEvent("takeover_lock_acquired", nil)
			if err = j.Apply(context.Background(), intentSender(func(context.Context, Intent) error { return nil }), "upsert", json.RawMessage(`{"docs":[{"id":"replacement"}]}`)); err != nil {
				t.Fatal("replacement journal commit failed", err)
			}
			partitionEvent("takeover_committed", nil)
			for {
				time.Sleep(time.Second) // Keep ownership until the controller stops us.
			}
		}
		partitionEvent("lock_retained", nil)
		return
	}
	j, err := OpenJournal(path, "partition-probe", phase == "owner")
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if phase != "owner" {
		state, err := j.Load()
		want := uint64(1)
		if phase == "verify-acquired" {
			want = 2
		}
		if err != nil || state.AcceptedSequence != want || state.Pending != nil || !state.ManagedIDs["baseline"] || (phase == "verify-acquired" && !state.ManagedIDs["replacement"]) {
			t.Fatal("post-partition journal regressed or is invalid", err)
		}
		partitionEvent("partition_verified", map[string]any{"accepted_sequence": state.AcceptedSequence})
		return
	}
	if err = j.Apply(context.Background(), intentSender(func(context.Context, Intent) error { return nil }), "upsert", json.RawMessage(`{"docs":[{"id":"baseline"}]}`)); err != nil {
		t.Fatal(err)
	}
	if err = j.SaveData(json.RawMessage(`{"writer":"baseline"}`)); err != nil {
		t.Fatal(err)
	}
	partitionEvent("owner_ready", nil)
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	origin := os.Getenv("ECS_AGENT_URI")
	// Restore even when start's response is lost. This task-local fault cannot
	// affect another task or shared VPC rules. Controller cleanup also stops us.
	restored := false
	defer func() {
		if !restored {
			if err := partitionFault(client, origin, "stop"); err != nil {
				t.Error("fault cleanup was not confirmed")
			}
		}
	}()
	if err = partitionFault(client, origin, "start"); err != nil {
		t.Fatal(err)
	}
	partitionEvent("fault_started", nil)
	result := make(chan error, 1)
	go func() {
		err := j.SaveData(json.RawMessage(`{"writer":"original-blocked"}`))
		partitionEvent("initial_write_completed", map[string]any{"write_succeeded": err == nil})
		result <- err
	}()
	select {
	case err = <-result:
		partitionEvent("io_not_blocked", map[string]any{"write_succeeded": err == nil})
		// Wait past the documented per-container fault API rate limit.
		time.Sleep(6 * time.Second)
		t.Fatal("NFS write did not remain blocked; fault effect is unproven")
	case <-time.After(6 * time.Second):
		partitionEvent("io_blocked", nil)
	}
	timer := time.NewTimer(partitionWindow - 6*time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
holding:
	for {
		select {
		case <-ticker.C:
			partitionEvent("owner_alive", nil)
		case <-timer.C:
			break holding
		}
	}
	if err = partitionFault(client, origin, "stop"); err != nil {
		t.Fatal(err)
	}
	restored = true
	partitionEvent("fault_restored", nil)
	select {
	case err = <-result:
		partitionEvent("blocked_write_result", map[string]any{"write_succeeded": err == nil})
	case <-time.After(3 * time.Minute):
		t.Fatal("blocked write did not resolve after restoration")
	}
	// This explicitly probes the old handle even after an I/O error. A success
	// proves a storage fencing gap, not that the production worker ignores errors.
	err = j.SaveData(json.RawMessage(`{"writer":"original-after-reconnect"}`))
	partitionEvent("old_write_result", map[string]any{"write_succeeded": err == nil})
	for {
		<-ticker.C
		partitionEvent("owner_alive_after_restore", nil)
	}
}

type partitionTransport func(*http.Request) (*http.Response, error)

func (f partitionTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestPartitionFaultRequestIsTaskLocalAndBounded(t *testing.T) {
	calls := 0
	client := &http.Client{Transport: partitionTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		var body map[string]any
		if r.Method != "POST" || json.NewDecoder(r.Body).Decode(&body) != nil || len(body) != 3 || body["Port"] != float64(2049) || body["Protocol"] != "tcp" || body["TrafficType"] != "egress" || r.Header.Get("Content-Type") != "application/json" {
			t.Fatal("fault escaped its NFS-only request")
		}
		status := "running"
		if strings.HasSuffix(r.URL.Path, "/stop") {
			status = "stopped"
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(fmt.Sprintf(`{"Status":%q}`, status)))}, nil
	})}
	for _, action := range []string{"start", "stop"} {
		if err := partitionFault(client, "http://169.254.170.23/v1/task", action); err != nil {
			t.Fatal(err)
		}
	}
	for _, invalid := range []string{"https://public.example", "http://127.0.0.1", "http://169.254.170.23?key=private", "http://user@169.254.170.23"} {
		if partitionFault(client, invalid, "start") == nil {
			t.Fatal("non-agent fault endpoint accepted")
		}
	}
	if calls != 2 || partitionFault(client, "http://169.254.170.23", "unknown") == nil {
		t.Fatal("invalid request reached transport")
	}
}

func TestPartitionFaultRequiresConfirmedAgentResponse(t *testing.T) {
	for _, status := range []int{200, 403, 500} {
		client := &http.Client{Transport: partitionTransport(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(`{"Status":"not-running"}`))}, nil
		})}
		if partitionFault(client, "http://169.254.170.23/task", "start") == nil {
			t.Fatal("unconfirmed fault was treated as active")
		}
	}
}
