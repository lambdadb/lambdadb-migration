package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestInventoryMaintenanceDoesNotBootstrapMissingState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.db")
	var output bytes.Buffer
	if err := maintainInventory(path, true, "", &output); err == nil {
		t.Fatal("missing state inspected")
	}
	manifest := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.WriteFile(manifest, []byte(`{"unexpected":"private-payload"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := maintainInventory(path, false, manifest, &output); err == nil {
		t.Fatal("invalid manifest applied")
	}
	if output.Len() != 0 {
		t.Fatal("failed maintenance wrote a partial/secret-bearing report")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("maintenance bootstrapped missing state")
	}
}
