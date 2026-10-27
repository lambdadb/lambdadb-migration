package connections

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func TestJournalRequiresExplicitBootstrapAndRetainsExactIntent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "connection.db")
	if _, err := OpenJournal(path, "binding", false); err == nil {
		t.Fatal("missing volume silently initialized")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("normal open created a file")
	}
	j, err := OpenJournal(path, "binding", true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = OpenJournal(path, "binding", true); err == nil {
		t.Fatal("bootstrap overwrote state")
	}
	if _, err = OpenJournal(path, "binding", false); err == nil {
		t.Fatal("second process admitted on same volume")
	}
	intent, err := j.Prepare("upsert", json.RawMessage(` { "docs": [{"id":"one","n":9223372036854775807,"title":"<html> & 한국어"}] } `))
	if err != nil {
		t.Fatal(err)
	}
	intent.Content[0] = '[' // No caller-owned slice may change the durable intent.
	s, err := j.Load()
	if err != nil {
		t.Fatal(err)
	}
	saved := *s.Pending
	if saved.Sequence != 1 || saved.Content[0] != '{' {
		t.Fatal("immutable intent changed")
	}
	if _, err = j.Prepare("delete", json.RawMessage(`{"ids":["one"]}`)); !errors.Is(err, ErrPending) {
		t.Fatal("pending operation overwritten")
	}
	changed := saved
	changed.Sequence = 2
	if j.Accept(changed) == nil {
		t.Fatal("wrong acknowledgment advanced state")
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenJournal(path, "another", false); err == nil {
		t.Fatal("configuration mismatch accepted")
	}
	j, err = OpenJournal(path, "binding", false)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	s, err = j.Load()
	if err != nil || s.Pending.Digest != saved.Digest || string(s.Pending.Content) != string(saved.Content) {
		t.Fatal("restart lost intent identity", err)
	}
	if err = acceptPrepared(j, saved); err != nil {
		t.Fatal(err)
	}
	next, err := j.Prepare("delete", json.RawMessage(`{"ids":["one"]}`))
	if err != nil || next.Sequence != 2 {
		t.Fatal("accepted sequence did not advance")
	}
	if err = j.SaveData(json.RawMessage(`{"phase":"applying","source_position":{"source_type":"postgresql","offset":"0/ABC"}}`)); err != nil {
		t.Fatal(err)
	}
	s, err = j.Load()
	if err != nil || s.Pending.Sequence != 2 || s.AcceptedSequence != 1 {
		t.Fatal("source state update lost intent", err)
	}
}

// The helper is this test binary, not a shell command or a private fixture.
func TestJournalProcessHelper(t *testing.T) {
	path := os.Getenv("CONNECTION_JOURNAL_PROCESS_PATH")
	if path == "" {
		return
	}
	j, err := OpenJournal(path, "process-binding", false)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := j.Prepare("delete", json.RawMessage(`{"ids":["one"]}`))
	if err != nil {
		t.Fatal(err)
	}
	if err = j.markSending(*intent); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("CONNECTION_JOURNAL_ACCEPTED") == "1" {
		if err = j.Accept(*intent); err != nil {
			t.Fatal(err)
		}
	}
	fmt.Println("journal-durable")
	select {} // Parent sends SIGKILL after the write transaction completed.
}
func TestJournalSurvivesRealProcessDeathBeforeAndAfterAcceptance(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(fmt.Sprint(accepted), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.db")
			j, err := OpenJournal(path, "process-binding", true)
			if err != nil {
				t.Fatal(err)
			}
			_ = j.Close()
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(binary, "-test.run=^TestJournalProcessHelper$", "-test.timeout=30s")
			cmd.Env = append(os.Environ(), "CONNECTION_JOURNAL_PROCESS_PATH="+path)
			if accepted {
				cmd.Env = append(cmd.Env, "CONNECTION_JOURNAL_ACCEPTED=1")
			}
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cmd.Process.Kill() })
			scanner := bufio.NewScanner(stdout)
			ready := false
			for scanner.Scan() {
				if scanner.Text() == "journal-durable" {
					ready = true
					break
				}
			}
			if !ready {
				_ = cmd.Wait()
				t.Fatal("child did not persist journal")
			}
			if err = cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = cmd.Wait()
			j, err = OpenJournal(path, "process-binding", false)
			if err != nil {
				t.Fatal(err)
			}
			defer j.Close()
			s, err := j.Load()
			if err != nil {
				t.Fatal(err)
			}
			if accepted {
				if s.Pending != nil || s.AcceptedSequence != 1 {
					t.Fatal("durable acceptance lost")
				}
			} else {
				if s.Pending == nil || s.Pending.Sequence != 1 || s.AcceptedSequence != 0 {
					t.Fatal("uncertain intent lost")
				}
			}
		})
	}
}

func TestTruncatedJournalIsNotInitializedOnRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "connection.db")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenJournal(path, "binding", false); err == nil {
		t.Fatal("truncated journal initialized")
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() != 0 {
		t.Fatal("failed restart modified truncated journal")
	}
}

func TestManagedInventorySurvivesPartialSourceCheckpointAndPendingRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	j, err := OpenJournal(path, "binding", true)
	if err != nil {
		t.Fatal(err)
	}
	if err = j.SaveData(json.RawMessage(`{"source_checkpoint":"before-new-row"}`)); err != nil {
		t.Fatal(err)
	}
	first, err := j.Prepare("upsert", json.RawMessage(`{"docs":[{"id":"old","title":"before"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if err = acceptPrepared(j, *first); err != nil {
		t.Fatal(err)
	}
	pending, err := j.Prepare("upsert", json.RawMessage(`{"docs":[{"id":"partial","title":"not-in-source-checkpoint"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	before, _ := j.Load()
	if before.ManagedIDs["partial"] {
		t.Fatal("unacknowledged document certified as accepted")
	}
	_ = j.Close()
	j, err = OpenJournal(path, "binding", false)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if err = acceptPrepared(j, *pending); err != nil {
		t.Fatal(err)
	}
	state, err := j.Load()
	if err != nil || !state.ManagedIDs["old"] || !state.ManagedIDs["partial"] || string(state.Data) != `{"source_checkpoint":"before-new-row"}` {
		t.Fatal("partial target row was lost from resync ownership inventory", err)
	}
	removal, err := j.Prepare("delete", json.RawMessage(`{"ids":["partial"]}`))
	if err != nil {
		t.Fatal(err)
	}
	if err = acceptPrepared(j, *removal); err != nil {
		t.Fatal(err)
	}
	state, _ = j.Load()
	if len(state.ManagedIDs) != 1 || !state.ManagedIDs["old"] {
		t.Fatal("accepted delete did not update inventory atomically")
	}
}

func TestJournalRejectsUntrackableIntentBeforeNetworkSend(t *testing.T) {
	j, err := OpenJournal(filepath.Join(t.TempDir(), "state.db"), "binding", true)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	for _, invalid := range []struct{ op, body string }{{"upsert", `{"docs":[{"id":1}]}`}, {"upsert", `{"docs":[{"id":"one"},{"id":"two"}]}`}, {"delete", `{"ids":["one","two"]}`}, {"delete", `{"ids":["one"],"docs":null}`}} {
		if _, err = j.Prepare(invalid.op, json.RawMessage(invalid.body)); err == nil {
			t.Fatal("untrackable intent accepted", invalid.body)
		}
	}
	state, err := j.Load()
	if err != nil || state.Pending != nil || state.AcceptedSequence != 0 || len(state.ManagedIDs) != 0 {
		t.Fatal("rejected intent changed regional state")
	}
}

// Simulate a successful response after a persisted transmission marker.
func acceptPrepared(j *Journal, p Intent) error {
	if err := j.markSending(p); err != nil {
		return err
	}
	return j.Accept(p)
}

func TestOldOrderedJournalCannotBeReinterpretedOrInitialized(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	j, err := OpenJournal(path, "binding", true)
	if err != nil {
		t.Fatal(err)
	}
	if err = j.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("connection"))
		var state State
		if err := json.Unmarshal(b.Get([]byte("state")), &state); err != nil {
			return err
		}
		state.Format = 2
		raw, _ := json.Marshal(state)
		return b.Put([]byte("state"), raw)
	}); err != nil {
		t.Fatal(err)
	}
	_ = j.Close()
	if _, err = OpenJournal(path, "binding", false); err == nil {
		t.Fatal("old ordered journal silently resumed")
	}
	if _, err = OpenJournal(path, "binding", true); err == nil {
		t.Fatal("old journal overwritten by initialization")
	}
}
