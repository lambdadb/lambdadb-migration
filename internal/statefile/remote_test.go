package statefile_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/lambdadb/lambdadb-migration/internal/statefile"
	"github.com/lambdadb/lambdadb-migration/internal/testutil/stateobjects"
	bolt "go.etcd.io/bbolt"
)

func remoteFixture(t *testing.T) (*stateobjects.Store, statefile.Binding, *statefile.Replica) {
	t.Helper()
	objects := stateobjects.New()
	marker, err := statefile.InitializeStore(t.Context(), objects, "test/", "ap-northeast-2")
	if err != nil {
		t.Fatal(err)
	}
	binding := statefile.Binding{StoreID: marker.ID, Region: marker.Region, ConnectionID: "connection", Fingerprint: strings.Repeat("a", 64)}
	r, err := statefile.OpenReplica(t.Context(), objects, "test/", binding, 1)
	if err != nil {
		t.Fatal(err)
	}
	return objects, binding, r
}

func localFile(t *testing.T, image statefile.Image) *statefile.DB {
	t.Helper()
	raw, err := bolt.Open(filepath.Join(t.TempDir(), "state.db"), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.Update(func(tx *bolt.Tx) error { return statefile.Import(tx, image) }); err != nil {
		t.Fatal(err)
	}
	db := statefile.Guard(raw)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func initialFiles() map[string]statefile.Image {
	return map[string]statefile.Image{
		"connection": {"journal": {"value": []byte(`{"integer":9007199254740993,"content":{"z":1,"a":2}}`)}},
		"source":     {"rows": {"value": []byte(`{"rows":{"1":"before"},"lsn":100}`)}},
	}
}

func TestRemoteRecoveryRebuildsBothFilesAcrossCheckpoint(t *testing.T) {
	objects, binding, r := remoteFixture(t)
	files := initialFiles()
	if err := r.Initialize(files); err != nil {
		t.Fatal(err)
	}
	db := localFile(t, files["source"])
	if err := db.Attach(r, "source"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 130; i++ {
		value := fmt.Sprintf(`{"rows":{"1":"after-%d"},"lsn":%d}`, i, i+101)
		if err := db.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("rows")).Put([]byte("value"), []byte(value)) }); err != nil {
			t.Fatal(err)
		}
	}
	want, err := statefile.Export(db)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := statefile.OpenReplica(t.Context(), objects, "test/", binding, 2)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := fresh.Recover()
	if err != nil || !reflect.DeepEqual(recovered["source"], want) || !reflect.DeepEqual(recovered["connection"], files["connection"]) {
		t.Fatal("checkpoint/log recovery changed exact state bytes", err)
	}
	dir, err := statefile.Materialize(t.TempDir(), recovered)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"connection", "source"} {
		raw, err := bolt.Open(filepath.Join(dir, name+".db"), 0600, nil)
		if err != nil {
			t.Fatal(err)
		}
		cache := statefile.Guard(raw)
		if err := cache.Attach(fresh, name); err != nil {
			t.Fatal(err)
		}
		cache.Close()
	}
	if err := db.Update(func(*bolt.Tx) error { return nil }); err != nil {
		// A no-op reads no fresh authority and permits no external operation.
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("rows")).Put([]byte("value"), []byte("stale-owner")) }); !errors.Is(err, statefile.ErrUnavailable) {
		t.Fatal("stale owner committed after handoff", err)
	}
}

func TestRemoteUnknownPublicationStopsBothHandlesAndRecoversAcceptedBytes(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprint(after), func(t *testing.T) {
			objects, binding, r := remoteFixture(t)
			files := initialFiles()
			if err := r.Initialize(files); err != nil {
				t.Fatal(err)
			}
			source, journal := localFile(t, files["source"]), localFile(t, files["connection"])
			if err := source.Attach(r, "source"); err != nil {
				t.Fatal(err)
			}
			if err := journal.Attach(r, "connection"); err != nil {
				t.Fatal(err)
			}
			objects.Fault = func(key string, committed bool) error {
				if strings.HasSuffix(key, "head.json") && after == committed {
					return errors.New("lost response")
				}
				return nil
			}
			if err := journal.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("journal")).Put([]byte("value"), []byte("accepted")) }); !errors.Is(err, statefile.ErrUnavailable) {
				t.Fatal("unknown remote write was allowed", err)
			}
			if err := source.View(func(*bolt.Tx) error { t.Fatal("other state handle survived uncertainty"); return nil }); !errors.Is(err, statefile.ErrUnavailable) {
				t.Fatal(err)
			}
			objects.Fault = nil
			fresh, err := statefile.OpenReplica(t.Context(), objects, "test/", binding, 2)
			if err != nil {
				t.Fatal(err)
			}
			got, err := fresh.Recover()
			if err != nil {
				t.Fatal(err)
			}
			want := files["connection"]["journal"]["value"]
			if after {
				want = []byte("accepted")
			}
			if string(got["connection"]["journal"]["value"]) != string(want) {
				t.Fatal("recovered state did not follow published head")
			}
		})
	}
}

func TestRemoteRefusesWrongIdentityMissingStateAndCorruptCheckpoint(t *testing.T) {
	objects, binding, r := remoteFixture(t)
	if _, err := r.Recover(); err == nil {
		t.Fatal("missing head initialized on restart")
	}
	if err := r.Initialize(initialFiles()); err != nil {
		t.Fatal(err)
	}
	if err := r.Initialize(initialFiles()); err == nil {
		t.Fatal("existing head initialized again")
	}
	wrong := binding
	wrong.StoreID = strings.Repeat("0", 32)
	if _, err := statefile.OpenReplica(t.Context(), objects, "test/", wrong, 1); err == nil {
		t.Fatal("wrong store adopted")
	}
	raw, _, _ := objects.Get(t.Context(), "test/workers/connection/head.json", 65536)
	var h struct {
		Checkpoint struct {
			Key string `json:"key"`
		} `json:"checkpoint"`
	}
	if json.Unmarshal(raw, &h) != nil {
		t.Fatal("invalid test head")
	}
	objects.Corrupt("test/workers/connection/" + h.Checkpoint.Key)
	fresh, err := statefile.OpenReplica(t.Context(), objects, "test/", binding, 2)
	if err != nil {
		t.Fatal(err)
	}
	before := objects.Puts
	if _, err := fresh.Recover(); err == nil || objects.Puts != before {
		t.Fatal("corrupt checkpoint was claimed or reconstructed")
	}
}
