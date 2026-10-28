// Package statefile prevents reuse of a database handle after a storage failure.
package statefile

import (
	"errors"
	"sync"

	bolt "go.etcd.io/bbolt"
)

// ErrUnavailable deliberately omits paths, stored values and underlying errors.
var ErrUnavailable = errors.New("regional state storage unavailable; stop and inspect retained files")

type transactions interface {
	View(func(*bolt.Tx) error) error
	Update(func(*bolt.Tx) error) error
	Close() error
}

// DB serializes access so an already waiting caller cannot begin another
// transaction after an I/O failure. It does not fence another process or repair
// a failed mmap, and a blocked storage call still requires process termination.
type DB struct {
	mu     sync.Mutex
	db     transactions
	failed bool
}

func Guard(db *bolt.DB) *DB { return &DB{db: db} }

func (d *DB) Err() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failed {
		return ErrUnavailable
	}
	return nil
}

func (d *DB) transaction(fn func(*bolt.Tx) error, write bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failed {
		return ErrUnavailable
	}
	var callbackErr error
	callback := func(tx *bolt.Tx) error { callbackErr = fn(tx); return callbackErr }
	var err error
	if write {
		err = d.db.Update(callback)
	} else {
		err = d.db.View(callback)
	}
	// A callback rejection (validation, authority or pending intent) is not an
	// I/O failure. A begin/commit failure invalidates this process's handle.
	if err != nil && callbackErr == nil {
		d.failed = true
		return ErrUnavailable
	}
	return err
}

func (d *DB) View(fn func(*bolt.Tx) error) error   { return d.transaction(fn, false) }
func (d *DB) Update(fn func(*bolt.Tx) error) error { return d.transaction(fn, true) }
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.failed = true
	return d.db.Close()
}
