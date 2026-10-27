package connections

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lambdadb/lambdadb-migration/internal/postgrescdc"
	bolt "go.etcd.io/bbolt"
)

// TestStateVolumeProbe is a disposable, cross-process storage experiment. It is
// compiled into a separate test image, never the worker executable. Its caller
// owns a fresh directory and must terminate the holder without closing the DBs.
func TestStateVolumeProbe(t *testing.T) {
	dir, phase := os.Getenv("CONNECTION_VOLUME_PROBE_DIR"), os.Getenv("CONNECTION_VOLUME_PROBE_PHASE")
	if dir == "" && phase == "" {
		t.Skip("explicit disposable volume probe required")
	}
	if !filepath.IsAbs(dir) || (phase != "hold" && phase != "contend" && phase != "verify") {
		t.Fatal("absolute disposable directory and known phase required")
	}
	journalPath, sourcePath := filepath.Join(dir, "connection.db"), filepath.Join(dir, "source.db")
	if phase == "hold" {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal("probe directory must be fresh", err)
		}
	}
	if phase == "contend" {
		for _, path := range []string{journalPath, sourcePath} {
			if info, err := os.Stat(path); err != nil || info.Size() == 0 {
				t.Fatal("holder's state is not visible")
			}
		}
		if j, err := OpenJournal(journalPath, "volume-probe", false); err == nil {
			_ = j.Close()
			t.Fatal("duplicate journal owner admitted")
		}
		if s, err := postgrescdc.OpenExistingStore(sourcePath); !errors.Is(err, bolt.ErrTimeout) {
			if s != nil {
				_ = s.Close()
			}
			t.Fatal("source contention did not return a lock timeout", err)
		}
		fmt.Println("STORAGE_PROBE_CONTENDED")
		return
	}
	j, err := OpenJournal(journalPath, "volume-probe", phase == "hold")
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	var source *postgrescdc.Store
	if phase == "hold" {
		source, err = postgrescdc.OpenNewStore(sourcePath)
	} else {
		source, err = postgrescdc.OpenExistingStore(sourcePath)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if phase == "hold" {
		if err = source.BindSource("volume-probe", true); err != nil {
			t.Fatal(err)
		}
		if err = source.Save(&postgrescdc.State{Format: 1, Fingerprint: "capture-probe", SystemID: "synthetic", LSN: 100,
			Columns: []postgrescdc.Column{{Name: "pk", OID: 20, Key: true}},
			Rows:    map[string]postgrescdc.Row{"9007199254740993": {"pk": json.RawMessage(`9007199254740993`)}}}); err != nil {
			t.Fatal(err)
		}
		if err = source.RecordInitialContinuous("volume-probe"); err != nil {
			t.Fatal(err)
		}
		if err = j.Apply(context.Background(), intentSender(func(context.Context, Intent) error { return nil }), "upsert", json.RawMessage(`{"docs":[{"id":"accepted"}]}`)); err != nil {
			t.Fatal(err)
		}
		intent, err := j.Prepare("delete", json.RawMessage(`{"ids":["accepted"]}`))
		if err != nil {
			t.Fatal(err)
		}
		if err = j.markSending(*intent); err != nil {
			t.Fatal(err)
		}
		fmt.Println("STORAGE_PROBE_READY")
		for {
			time.Sleep(time.Second) // Caller kills this process; no normal Close.
		}
	}
	metadata, err := source.Inspect("volume-probe", "continuous")
	if err != nil || metadata.LSN != "0/64" || metadata.Rows != 1 || metadata.CurrentRevision == nil {
		t.Fatal("source checkpoint/cut did not survive", err)
	}
	saved, err := source.Load()
	if err != nil || string(saved.Rows["9007199254740993"]["pk"]) != "9007199254740993" {
		t.Fatal("source row image changed", err)
	}
	s, err := j.Load()
	if err != nil || s.AcceptedSequence != 1 || !s.ManagedIDs["accepted"] || len(s.ManagedIDs) != 1 || s.Pending == nil || s.Pending.Sequence != 2 || s.Pending.Status != "sending" {
		t.Fatal("accepted inventory or uncertain intent did not survive", err)
	}
	calls := 0
	err = j.RecoverPending(context.Background(), intentSender(func(context.Context, Intent) error { calls++; return nil }))
	if !errors.Is(err, ErrTargetOutcomeUncertain) || calls != 0 {
		t.Fatal("uncertain request was retransmitted")
	}
	fmt.Println("STORAGE_PROBE_VERIFIED")
}

// This local process check validates the harness in the normal suite. It does
// not certify NFS locking, cross-host ownership or EFS failure behavior.
func TestStateVolumeProbeLocal(t *testing.T) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dir := filepath.Join(t.TempDir(), "probe")
	command := func(phase string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, binary, "-test.run=^TestStateVolumeProbe$", "-test.timeout=25s")
		cmd.Env = append(os.Environ(), "CONNECTION_VOLUME_PROBE_DIR="+dir, "CONNECTION_VOLUME_PROBE_PHASE="+phase)
		return cmd
	}
	holder := command("hold")
	stdout, err := holder.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	holder.Stderr = os.Stderr
	if err = holder.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Process.Kill(); _ = holder.Wait() }()
	scanner := bufio.NewScanner(stdout)
	ready := false
	for scanner.Scan() {
		if scanner.Text() == "STORAGE_PROBE_READY" {
			ready = true
			break
		}
	}
	if !ready {
		t.Fatal("holder did not commit probe state")
	}
	if out, err := command("contend").CombinedOutput(); err != nil || !strings.Contains(string(out), "STORAGE_PROBE_CONTENDED") {
		t.Fatalf("duplicate-owner probe failed: %v %s", err, out)
	}
	if err = holder.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = holder.Wait(); err == nil {
		t.Fatal("holder was not killed")
	}
	if out, err := command("verify").CombinedOutput(); err != nil || !strings.Contains(string(out), "STORAGE_PROBE_VERIFIED") {
		t.Fatalf("post-kill probe failed: %v %s", err, out)
	}
}
