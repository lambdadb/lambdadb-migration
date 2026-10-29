package statefile

import (
	"errors"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

// LockCache serializes users of one local cache directory before a remote claim.
// Cross-host ownership still requires the execution controller's stopped proof.
func LockCache(directory string) (*bolt.DB, error) {
	if directory == "" {
		return nil, errors.New("local cache directory required")
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, errors.New("local cache directory unavailable")
	}
	lock, err := bolt.Open(filepath.Join(directory, "cache.lock"), 0600, &bolt.Options{Timeout: 100 * time.Millisecond})
	if err != nil {
		return nil, errors.New("local cache is exclusively owned or unavailable")
	}
	return lock, nil
}

// Materialize creates a private pair and returns it only after both files are
// complete and synced. Existing files and inspection archives are never adopted.
func Materialize(directory string, files map[string]Image) (string, error) {
	if err := validateFiles(files); err != nil {
		return "", err
	}
	local, err := os.MkdirTemp(directory, "cache-")
	if err != nil {
		return "", errors.New("local recovery directory unavailable")
	}
	for _, name := range []string{"connection", "source"} {
		file := filepath.Join(local, name+".db")
		db, err := bolt.Open(file, 0600, nil)
		if err != nil {
			return "", errors.New("local recovery file unavailable")
		}
		err = db.Update(func(tx *bolt.Tx) error { return Import(tx, files[name]) })
		closeErr := db.Close()
		if err != nil || closeErr != nil || SyncParents(file) != nil {
			return "", errors.New("local recovery publication failed")
		}
	}
	return local, nil
}
