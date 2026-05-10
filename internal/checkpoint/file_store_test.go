package checkpoint

import (
	"context"
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
