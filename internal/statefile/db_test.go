package statefile

import (
	"errors"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	bolt "go.etcd.io/bbolt"
)

type failedCommit struct {
	*bolt.DB
	committed chan struct{}
	release   chan struct{}
}

func (d *failedCommit) Update(fn func(*bolt.Tx) error) error {
	if err := d.DB.Update(fn); err != nil {
		return err
	}
	close(d.committed)
	<-d.release
	return syscall.EIO // Bytes survived, but the caller cannot assume acceptance.
}

func TestCommitFailureBlocksAlreadyWaitingReadersAndWriters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	raw, err := bolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	backend := &failedCommit{raw, make(chan struct{}), make(chan struct{})}
	db := &DB{db: backend}
	defer db.Close()
	done := make(chan error, 1)
	go func() {
		done <- db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucket([]byte("state"))
			if err != nil {
				return err
			}
			return b.Put([]byte("checkpoint"), []byte("retained"))
		})
	}()
	<-backend.committed
	var callers sync.WaitGroup
	errorsSeen := make(chan error, 2)
	for _, call := range []func(func(*bolt.Tx) error) error{db.View, db.Update} {
		callers.Add(1)
		go func(call func(func(*bolt.Tx) error) error) {
			defer callers.Done()
			errorsSeen <- call(func(*bolt.Tx) error { panic("failed handle reused") })
		}(call)
	}
	close(backend.release)
	if err := <-done; !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	callers.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if !errors.Is(err, ErrUnavailable) {
			t.Fatal(err)
		}
	}
	if !errors.Is(db.Err(), ErrUnavailable) {
		t.Fatal("failure not retained")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// A fresh handle can inspect the committed bytes. The failed handle remains
	// unusable regardless of whether its transaction happened to survive.
	fresh, err := bolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if err := fresh.View(func(tx *bolt.Tx) error {
		if string(tx.Bucket([]byte("state")).Get([]byte("checkpoint"))) != "retained" {
			t.Fatal("state changed")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCallbackRejectionDoesNotPoisonHealthyHandle(t *testing.T) {
	raw, err := bolt.Open(filepath.Join(t.TempDir(), "state.db"), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	db := Guard(raw)
	defer db.Close()
	rejected := errors.New("authority expired")
	if err := db.Update(func(*bolt.Tx) error { return rejected }); !errors.Is(err, rejected) {
		t.Fatal(err)
	}
	if err := db.View(func(*bolt.Tx) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if db.Err() != nil {
		t.Fatal("validation error invalidated handle")
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(*bolt.Tx) error { t.Fatal("closed database callback ran"); return nil }); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
}
