package statefile

import (
	"os"
	"path/filepath"
)

// SyncParents makes newly created file and directory names durable before they
// are advertised remotely. File contents must already have been synced.
func SyncParents(path string) error {
	path, err := filepath.Abs(path)
	if err != nil {
		return ErrUnavailable
	}
	for parent := filepath.Dir(path); ; parent = filepath.Dir(parent) {
		dir, err := os.Open(parent)
		if err != nil {
			return ErrUnavailable
		}
		err = dir.Sync()
		closeErr := dir.Close()
		if err != nil || closeErr != nil {
			return ErrUnavailable
		}
		if filepath.Dir(parent) == parent {
			return nil
		}
	}
}
