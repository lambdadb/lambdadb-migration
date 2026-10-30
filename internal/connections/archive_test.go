package connections

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func stoppedArchiveFixture(t *testing.T, pending bool) (string, string) {
	t.Helper()
	dir, j, source := inspectionFixture(t, "continuous")
	if pending {
		intent, err := j.Prepare("delete", json.RawMessage(`{"ids":["one"]}`))
		if err != nil {
			t.Fatal(err)
		}
		if err = j.markSending(*intent); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	return dir, filepath.Join(t.TempDir(), "state.tar.gz")
}

func assertRestoredWorkerBlocked(t *testing.T, dir string) {
	t.Helper()
	for _, initialize := range []bool{false, true} {
		control := &Control{Client: &http.Client{Transport: partitionTransport(func(*http.Request) (*http.Response, error) {
			t.Fatal("restore contacted regional control")
			return nil, nil
		})}}
		worker := Worker{Control: control, Secrets: AWSSecrets{}, StateDirectory: dir, Initialize: initialize}
		err := worker.Run(t.Context())
		if err == nil || !strings.Contains(err.Error(), "inspection-only") {
			t.Fatal("restored state could resume or initialize", err)
		}
	}
}

func TestStateArchiveRoundTripPreservesExactPairButNeverAuthorizesResume(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(map[bool]string{false: "clean-old-pair", true: "uncertain"}[pending], func(t *testing.T) {
			dir, path := stoppedArchiveFixture(t, pending)
			originals := map[string][]byte{}
			for _, name := range []string{"connection.db", "source.db"} {
				raw, err := os.ReadFile(filepath.Join(dir, name))
				if err != nil {
					t.Fatal(err)
				}
				originals[name] = raw
			}
			manifest, err := ArchiveState(dir, path)
			if err != nil {
				t.Fatal(err)
			}
			if manifest.ResumeAuthorized || manifest.Inspection.ResumeAuthorized || manifest.Inspection.RecoveryRequired != pending || len(manifest.Files) != 2 {
				t.Fatal("archive overstated authority or lost intent")
			}
			info, _ := os.Stat(path)
			if info.Mode().Perm() != 0600 {
				t.Fatal("archive is not private")
			}
			restored := filepath.Join(t.TempDir(), "inspection")
			result, err := RestoreState(path, restored)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(result, manifest) {
				t.Fatal("restored manifest changed")
			}
			assertRestoredWorkerBlocked(t, restored)
			for name, expected := range originals {
				source, _ := os.ReadFile(filepath.Join(dir, name))
				copied, _ := os.ReadFile(filepath.Join(restored, name))
				if !bytes.Equal(expected, source) || !bytes.Equal(expected, copied) {
					t.Fatal("original state modified or restored pair differs")
				}
			}
		})
	}
}

func TestStateArchiveRefusesActiveStateAndNeverOverwritesExistingPaths(t *testing.T) {
	dir, j, source := inspectionFixture(t, "continuous")
	archive := filepath.Join(t.TempDir(), "state.tar.gz")
	if _, err := ArchiveState(dir, archive); err == nil {
		t.Fatal("active journal archived")
	}
	_ = j.Close()
	if _, err := ArchiveState(dir, archive); err == nil {
		t.Fatal("active source archived")
	}
	_ = source.Close()
	if err := os.WriteFile(archive, []byte("preserve-existing"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ArchiveState(dir, archive); err == nil {
		t.Fatal("archive overwritten")
	}
	raw, _ := os.ReadFile(archive)
	if string(raw) != "preserve-existing" {
		t.Fatal("existing archive changed")
	}
	fresh := filepath.Join(t.TempDir(), "good.tar.gz")
	if _, err := ArchiveState(dir, fresh); err != nil {
		t.Fatal(err)
	}
	existing := t.TempDir()
	if _, err := RestoreState(fresh, existing); err == nil {
		t.Fatal("existing state directory adopted")
	}
	entries, _ := os.ReadDir(existing)
	if len(entries) != 0 {
		t.Fatal("existing state directory modified")
	}
}

func TestStateArchiveCRCFailureStaysQuarantined(t *testing.T) {
	dir, path := stoppedArchiveFixture(t, false)
	if _, err := ArchiveState(dir, path); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(path)
	raw[len(raw)-8] ^= 1
	bad := filepath.Join(t.TempDir(), "bad.tar.gz")
	_ = os.WriteFile(bad, raw, 0600)
	restored := filepath.Join(t.TempDir(), "inspection")
	if _, err := RestoreState(bad, restored); err == nil {
		t.Fatal("invalid gzip CRC accepted")
	}
	assertRestoredWorkerBlocked(t, restored)
}

func TestStateArchiveRejectsTraversalLinksAndDuplicateEntries(t *testing.T) {
	for _, kind := range []string{"traversal", "link", "duplicate"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "bad.tar.gz")
			file, _ := os.Create(path)
			gz := gzip.NewWriter(file)
			writer := tar.NewWriter(gz)
			header := &tar.Header{Name: "connection.db", Mode: 0600, Size: 1, Typeflag: tar.TypeReg}
			if kind == "traversal" {
				header.Name = "../escape"
			}
			if kind == "link" {
				header.Typeflag = tar.TypeSymlink
				header.Linkname = "../escape"
				header.Size = 0
			}
			if err := writer.WriteHeader(header); err != nil {
				t.Fatal(err)
			}
			if header.Size > 0 {
				_, _ = writer.Write([]byte("x"))
			}
			if kind == "duplicate" {
				_ = writer.WriteHeader(header)
				_, _ = writer.Write([]byte("x"))
			}
			_ = writer.Close()
			_ = gz.Close()
			_ = file.Close()
			parent := t.TempDir()
			restored := filepath.Join(parent, "inspection")
			if _, err := RestoreState(path, restored); err == nil {
				t.Fatal("unsafe archive accepted")
			}
			if _, err := os.Stat(filepath.Join(parent, "escape")); !os.IsNotExist(err) {
				t.Fatal("archive escaped inspection directory")
			}
			assertRestoredWorkerBlocked(t, restored)
		})
	}
}

func TestStateArchiveRejectsHashMismatchEvenWithValidCompression(t *testing.T) {
	dir, path := stoppedArchiveFixture(t, true)
	if _, err := ArchiveState(dir, path); err != nil {
		t.Fatal(err)
	}
	file, _ := os.Open(path)
	gz, _ := gzip.NewReader(file)
	reader := tar.NewReader(gz)
	bad := filepath.Join(t.TempDir(), "wrong-hash.tar.gz")
	output, _ := os.Create(bad)
	compressed := gzip.NewWriter(output)
	writer := tar.NewWriter(compressed)
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		raw, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		if header.Name == "manifest.json" {
			var manifest StateArchive
			if err = json.Unmarshal(raw, &manifest); err != nil {
				t.Fatal(err)
			}
			item := manifest.Files["source.db"]
			item.SHA256 = strings.Repeat("0", 64)
			manifest.Files["source.db"] = item
			raw, _ = json.Marshal(manifest)
			header.Size = int64(len(raw))
		}
		if err = writer.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if _, err = writer.Write(raw); err != nil {
			t.Fatal(err)
		}
	}
	_ = writer.Close()
	_ = compressed.Close()
	_ = output.Close()
	_ = gz.Close()
	_ = file.Close()
	restored := filepath.Join(t.TempDir(), "inspection")
	if _, err := RestoreState(bad, restored); err == nil {
		t.Fatal("hash mismatch accepted")
	}
	assertRestoredWorkerBlocked(t, restored)
}
