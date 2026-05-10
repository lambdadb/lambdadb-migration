package checkpoint

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileStoreSaveLoadDelete(t *testing.T) {
	ctx := context.Background()
	store := NewFileStore(t.TempDir())

	cp := Checkpoint{
		SourceKind:       "qdrant",
		SourceCollection: "articles",
		TargetCollection: "articles",
		AcceptedRecords:  42,
		UpdatedAt:        time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC),
	}

	if err := store.Save(ctx, "qdrant/articles", cp); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	got, err := store.Load(ctx, "qdrant/articles")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got == nil {
		t.Fatal("Load() returned nil checkpoint")
	}
	if got.AcceptedRecords != cp.AcceptedRecords {
		t.Fatalf("AcceptedRecords = %d, want %d", got.AcceptedRecords, cp.AcceptedRecords)
	}

	if err := store.Delete(ctx, "qdrant/articles"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	got, err = store.Load(ctx, "qdrant/articles")
	if err != nil {
		t.Fatalf("Load() after delete error = %v", err)
	}
	if got != nil {
		t.Fatalf("Load() after delete = %#v, want nil", got)
	}
}

func TestFileStoreLoadPreservesLargeNumericCursor(t *testing.T) {
	ctx := context.Background()
	store := NewFileStore(t.TempDir())
	path := store.path("qdrant/articles")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	data := []byte(`{
  "sourceKind": "qdrant",
  "sourceCollection": "articles",
  "targetCollection": "articles",
  "cursor": {"num": 18446744073709551615},
  "acceptedRecords": 42,
  "updatedAt": "2026-05-10T12:00:00Z"
}`)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	got, err := store.Load(ctx, "qdrant/articles")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	cursor, ok := got.Cursor.(map[string]any)
	if !ok {
		t.Fatalf("Cursor = %T, want map[string]any", got.Cursor)
	}
	num, ok := cursor["num"].(json.Number)
	if !ok {
		t.Fatalf("cursor num = %T, want json.Number", cursor["num"])
	}
	if got, want := num.String(), "18446744073709551615"; got != want {
		t.Fatalf("cursor num = %q, want %q", got, want)
	}
}
