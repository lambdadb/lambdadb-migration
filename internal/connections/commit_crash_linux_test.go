package connections

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Linux-only, opt-in subprocess test. strace supplies the syscall boundary;
// neither the journal nor its bbolt dependency gets a production fault hook.
func TestCommitCrashChild(t *testing.T) {
	path := os.Getenv("CONNECTION_COMMIT_CRASH_PATH")
	if path == "" {
		t.Skip("test subprocess only")
	}
	runtime.LockOSThread() // strace injection counts are per OS thread.
	defer runtime.UnlockOSThread()
	journal, err := OpenJournal(path, "commit-crash", false)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	state, err := journal.Load()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("commit-ready:%d\n", os.Getpid())
	if os.Getenv("CONNECTION_COMMIT_CRASH_MODE") == "accept" {
		err = journal.Accept(*state.Pending)
	} else {
		err = journal.SaveData(crashGrowthData())
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Fatal("commit completed without the required syscall stop")
}

func crashGrowthData() json.RawMessage {
	raw, _ := json.Marshal(map[string]any{"checkpoint": 2, "payload": strings.Repeat("x", 1<<20)})
	return raw
}

func TestCommitCrashAtDataAndMetadataSync(t *testing.T) {
	if os.Getenv("CONNECTION_TEST_COMMIT_CRASH") != "1" {
		t.Skip("explicit local Linux syscall qualification required")
	}
	if _, err := exec.LookPath("strace"); err != nil {
		t.Fatal("strace required by opt-in qualification")
	}
	for _, mode := range []string{"accept", "grow"} {
		for _, boundary := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s/sync-%d", mode, boundary), func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "connection.db")
				j, err := OpenJournal(path, "commit-crash", true)
				if err != nil {
					t.Fatal(err)
				}
				base := json.RawMessage(`{"docs":[{"id":"baseline"}]}`)
				if err = j.Apply(t.Context(), intentSender(func(_ context.Context, _ Intent) error { return nil }), "upsert", base); err != nil {
					t.Fatal(err)
				}
				if err = j.SaveData(json.RawMessage(`{"checkpoint":1}`)); err != nil {
					t.Fatal(err)
				}
				intent, err := j.Prepare("upsert", json.RawMessage(`{"docs":[{"id":"next"}]}`))
				if err != nil {
					t.Fatal(err)
				}
				if err = j.markSending(*intent); err != nil {
					t.Fatal(err)
				}
				if err = j.Close(); err != nil {
					t.Fatal(err)
				}
				initial, _ := os.Stat(path)
				trace := filepath.Join(t.TempDir(), "sync.trace")
				binary, _ := os.Executable()
				cmd := exec.Command("strace", "-f", "--kill-on-exit", "-o", trace, "-e", "trace=fdatasync", "-e", fmt.Sprintf("inject=fdatasync:signal=SIGSTOP:when=%d", boundary), binary, "-test.run=^TestCommitCrashChild$", "-test.timeout=20s")
				cmd.Env = append(os.Environ(), "CONNECTION_COMMIT_CRASH_PATH="+path, "CONNECTION_COMMIT_CRASH_MODE="+mode)
				cmd.Stderr = os.Stderr
				stdout, err := cmd.StdoutPipe()
				if err != nil {
					t.Fatal(err)
				}
				if err = cmd.Start(); err != nil {
					t.Fatal(err)
				}
				pid := 0
				waited := false
				defer func() {
					if pid > 0 {
						_ = syscall.Kill(pid, syscall.SIGKILL)
					}
					if !waited {
						_ = cmd.Process.Kill()
						_ = cmd.Wait()
					}
				}()
				ready := make(chan int, 1)
				go func() {
					scan := bufio.NewScanner(stdout)
					for scan.Scan() {
						if strings.HasPrefix(scan.Text(), "commit-ready:") {
							value, _ := strconv.Atoi(strings.TrimPrefix(scan.Text(), "commit-ready:"))
							ready <- value
							return
						}
					}
					ready <- 0
				}()
				select {
				case pid = <-ready:
				case <-time.After(10 * time.Second):
					t.Fatal("traced child readiness timed out")
				}
				if pid <= 0 {
					t.Fatal("traced child did not open journal; see tracer diagnostics")
				}
				deadline := time.Now().Add(5 * time.Second)
				stopped := false
				for time.Now().Before(deadline) {
					raw, _ := os.ReadFile(trace)
					stat, _ := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
					tail := string(stat)[strings.LastIndex(string(stat), ")")+1:]
					fields := strings.Fields(tail)
					if bytes.Contains(raw, []byte("SIGSTOP")) && len(fields) > 0 && (fields[0] == "T" || fields[0] == "t") {
						stopped = true
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
				if !stopped {
					raw, _ := os.ReadFile(trace)
					t.Fatalf("syscall stop unproven: %s", raw)
				}
				if err = syscall.Kill(pid, syscall.SIGKILL); err != nil {
					t.Fatal(err)
				}
				err = cmd.Wait()
				waited = true
				if err == nil {
					t.Fatal("child unexpectedly succeeded")
				}
				raw, _ := os.ReadFile(trace)
				if !bytes.Contains(raw, []byte("killed by SIGKILL")) || strings.Count(string(raw), "fdatasync(") != boundary {
					t.Fatalf("wrong syscall/signal boundary: %s", raw)
				}
				t.Logf("syscall evidence:\n%s", strings.TrimSpace(string(raw)))
				j, err = OpenJournal(path, "commit-crash", false)
				if err != nil {
					t.Fatal(err)
				}
				defer j.Close()
				got, err := j.Load()
				if err != nil {
					t.Fatal(err)
				}
				if err = j.db.View(func(tx *bolt.Tx) error {
					for e := range tx.Check() {
						if e != nil {
							return e
						}
					}
					return nil
				}); err != nil {
					t.Fatal("bbolt page integrity failed", err)
				}
				if mode == "accept" && boundary == 2 {
					if got.AcceptedSequence != 2 || got.Pending != nil || len(got.ManagedIDs) != 2 || !got.ManagedIDs["next"] {
						t.Fatal("complete accepted state lost")
					}
				} else {
					if got.AcceptedSequence != 1 || got.Pending == nil || got.Pending.Status != "sending" || !sameIntent(*intent, *got.Pending) || len(got.ManagedIDs) != 1 || !got.ManagedIDs["baseline"] {
						t.Fatal("pending evidence or inventory regressed")
					}
				}
				expected := json.RawMessage(`{"checkpoint":1}`)
				if mode == "grow" && boundary == 2 {
					expected = crashGrowthData()
				}
				if !bytes.Equal(got.Data, expected) {
					t.Fatal("partial checkpoint after interrupted commit")
				}
				final, _ := os.Stat(path)
				if mode == "grow" && final.Size() <= initial.Size() {
					t.Fatal("growth/remapping was not exercised")
				}
				t.Logf("actual SIGKILL at fdatasync %d; mode=%s; bytes=%d; accepted=%d; pending=%t; integrity valid", boundary, mode, final.Size(), got.AcceptedSequence, got.Pending != nil)
			})
		}
	}
}
