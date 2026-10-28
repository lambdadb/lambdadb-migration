package connections

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Test-only task-local NFS fault. It never opens the worker's databases, changes
// shared network rules, or restores a failed production database handle.
type nfsFaultProbe struct {
	enabled  bool
	active   bool
	started  time.Time
	done     chan error
	reported bool
	emit     func(string, int)
}

func (p *nfsFaultProbe) update(action string, pid int) error {
	if action == "" {
		return nil
	}
	if !p.enabled || (action != "start" && action != "stop") {
		return errors.New("explicit test-only NFS fault required")
	}
	if (action == "start") == p.active {
		p.observe(pid)
		return nil
	}
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	if action == "stop" {
		if err := partitionFault(client, os.Getenv("ECS_AGENT_URI"), "stop"); err != nil {
			return err
		}
		p.active = false
		p.emit("nfs_fault_stopped", pid)
		return nil
	}
	// Establish this separate canary on healthy storage before fault injection.
	file, err := os.OpenFile(filepath.Join("/state", "nfs-canary-"+strconv.Itoa(pid)), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return errors.New("NFS canary setup failed")
	}
	data := make([]byte, 4096)
	if _, err = file.WriteAt(data, 0); err == nil {
		err = file.Sync()
	}
	if err != nil {
		_ = file.Close()
		return errors.New("NFS canary baseline failed")
	}
	if err = partitionFault(client, os.Getenv("ECS_AGENT_URI"), "start"); err != nil {
		_ = file.Close()
		return err
	}
	p.active, p.started, p.done = true, time.Now(), make(chan error, 1)
	p.emit("nfs_fault_started", pid)
	go func() {
		defer file.Close()
		data[0] = 1
		_, err := file.WriteAt(data, 0)
		if err == nil {
			err = file.Sync()
		}
		p.done <- err
	}()
	return nil
}

func (p *nfsFaultProbe) observe(pid int) {
	if !p.active || p.reported || time.Since(p.started) < 6*time.Second {
		return
	}
	select {
	case <-p.done:
		p.reported = true
		p.emit("nfs_io_not_blocked", pid)
	default:
		// A blocked canary alone does not prove the actual worker is blocked.
		if workerInIOSleep(pid) {
			p.reported = true
			p.emit("nfs_actual_worker_io_blocked", pid)
		}
	}
}

func ioSleep(stat string) bool {
	end := strings.LastIndex(stat, ")")
	fields := strings.Fields(stat[end+1:])
	return end >= 0 && len(fields) > 0 && fields[0] == "D"
}
func workerInIOSleep(pid int) bool {
	threads, err := filepath.Glob("/proc/" + strconv.Itoa(pid) + "/task/*/stat")
	if err != nil || len(threads) > 128 {
		return false
	}
	for _, path := range threads {
		raw, err := os.ReadFile(path)
		if err == nil && len(raw) < 4096 && ioSleep(string(raw)) {
			return true
		}
	}
	return false
}

func TestStorageFaultProbeRequiresActualWorkerAndBlockedCanary(t *testing.T) {
	var events []string
	p := &nfsFaultProbe{active: true, started: time.Now().Add(-7 * time.Second), done: make(chan error, 1), emit: func(event string, _ int) { events = append(events, event) }}
	p.observe(-1)
	if len(events) != 0 {
		t.Fatal("missing worker was reported blocked")
	}
	p.done <- nil
	p.observe(-1)
	if len(events) != 1 || events[0] != "nfs_io_not_blocked" {
		t.Fatal(events)
	}
	p.observe(-1)
	if len(events) != 1 {
		t.Fatal("duplicate probe result")
	}
	if p.update("start", 1) == nil || p.update("unknown", 1) == nil {
		t.Fatal("unapproved fault accepted")
	}
}
func TestStorageFaultParsesOnlyUninterruptibleWorkerThreadState(t *testing.T) {
	for raw, want := range map[string]bool{"12 (worker (thread)) D 1 2": true, "12 (worker) S 1 2": false, "invalid": false, "12 (D) R 1 2": false} {
		if ioSleep(raw) != want {
			t.Fatal("incorrect worker I/O state")
		}
	}
}
