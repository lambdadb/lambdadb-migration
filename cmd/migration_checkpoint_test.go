package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/lambdadb/lambdadb-migration/internal/checkpoint"
	"github.com/lambdadb/lambdadb-migration/internal/config"
	"github.com/lambdadb/lambdadb-migration/internal/source"
)

func TestCompletedCheckpointValidation(t *testing.T) {
	for _, emptyFinalPage := range []bool{false, true} {
		t.Run(fmt.Sprintf("empty_final_page=%v", emptyFinalPage), func(t *testing.T) {
			var mu sync.Mutex
			writes, fetches := 0, 0
			mismatch := true
			var written map[string]any
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/projects/test/collections/articles/docs/upsert":
					var body struct {
						Docs []map[string]any `json:"docs"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Docs) != 1 {
						t.Errorf("write = %+v, %v", body, err)
						w.WriteHeader(400)
						return
					}
					writes++
					written = body.Docs[0]
					w.WriteHeader(http.StatusAccepted)
					fmt.Fprint(w, `{"message":"accepted"}`)
				case "/projects/test/collections/articles":
					fmt.Fprint(w, `{"collection":{"collectionName":"articles","numDocs":1}}`)
				case "/projects/test/collections/articles/docs/fetch":
					fetches++
					doc := cloneDocument(written)
					if mismatch {
						doc["title"] = "wrong"
					}
					json.NewEncoder(w).Encode(map[string]any{"total": 1, "took": 0, "isDocsInline": true, "docs": []any{map[string]any{"collection": "articles", "doc": doc}}})
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			src := &checkpointSource{emptyFinalPage: emptyFinalPage, records: []source.Record{{ID: "1", Payload: map[string]any{
				"title": "expected", "rank": int64(7), "dense": []float32{0.1, 0.2}, "sparse": map[string]float32{"3": 0.7},
			}}}}
			cfg := checkpointRunConfig(src, server.URL, t.TempDir())
			cfg.Migration.Validate = true
			cfg.Migration.ValidationSampleSize = 1
			cfg.Migration.CleanupCheckpoint = true
			cfg.Migration.ValidationReport = filepath.Join(t.TempDir(), "validation.json")
			ctx := context.Background()
			store := checkpoint.NewFileStore(cfg.Migration.CheckpointPath)
			key := sourceCheckpointKey("qdrant", "source", "test", "articles")
			for attempt := 0; attempt < 2; attempt++ {
				err := runMigration(ctx, cfg)
				if err == nil || !strings.Contains(err.Error(), `field "title" mismatch`) {
					t.Fatalf("validation error = %v", err)
				}
				cp, err := store.Load(ctx, key)
				if err != nil || cp == nil || !cp.SourceDone || cp.AcceptedRecords != 1 || len(cp.ValidationSamples) != 1 {
					t.Fatalf("checkpoint = %+v, %v", cp, err)
				}
			}
			mu.Lock()
			mismatch = false
			mu.Unlock()
			if err := runMigration(ctx, cfg); err != nil {
				t.Fatalf("validation retry: %v", err)
			}
			if cp, err := store.Load(ctx, key); cp != nil || err != nil {
				t.Fatalf("cleanup = %+v, %v", cp, err)
			}
			data, err := os.ReadFile(cfg.Migration.ValidationReport)
			if err != nil {
				t.Fatal(err)
			}
			var report validationReport
			if err := json.Unmarshal(data, &report); err != nil {
				t.Fatal(err)
			}
			if report.Status != "pass" || report.Samples.Compared != 1 || report.Samples.Skipped {
				t.Fatalf("report = %+v", report)
			}
			wantReads := 1
			if emptyFinalPage {
				wantReads = 2
			}
			mu.Lock()
			defer mu.Unlock()
			if src.reads != wantReads || writes != 1 || fetches != 3 {
				t.Fatalf("reads/writes/fetches = %d/%d/%d", src.reads, writes, fetches)
			}
		})
	}
}

func TestCheckpointCompletionCompatibility(t *testing.T) {
	for _, tt := range []struct {
		name   string
		legacy bool
	}{
		{name: "empty_source"}, {name: "legacy_checkpoint", legacy: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			src := &checkpointSource{}
			cfg := checkpointRunConfig(src, "http://unused.invalid", t.TempDir())
			key := sourceCheckpointKey("qdrant", "source", "test", "articles")
			store := checkpoint.NewFileStore(cfg.Migration.CheckpointPath)
			ctx := context.Background()
			if tt.legacy {
				// No sourceDone field, even with acceptedRecords equal to inventory.
				path := filepath.Join(cfg.Migration.CheckpointPath, key+".json")
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(`{"sourceKind":"qdrant","sourceCollection":"source","targetCollection":"articles","acceptedRecords":0,"cursor":"end"}`), 0600); err != nil {
					t.Fatal(err)
				}
			}
			for run := 0; run < 2; run++ {
				if err := runMigration(ctx, cfg); err != nil {
					t.Fatal(err)
				}
				cp, err := store.Load(ctx, key)
				if err != nil || cp == nil || !cp.SourceDone || cp.AcceptedRecords != 0 {
					t.Fatalf("checkpoint = %+v, %v", cp, err)
				}
			}
			if src.reads != 1 {
				t.Fatalf("source reads = %d, want 1", src.reads)
			}
		})
	}
}

func TestCompletedCheckpointCannotSilentlySkipRequestedSamples(t *testing.T) {
	src := &checkpointSource{}
	cfg := checkpointRunConfig(src, "http://unused.invalid", t.TempDir())
	cfg.Migration.Validate = true
	cfg.Migration.ValidationSampleSize = 1
	store := checkpoint.NewFileStore(cfg.Migration.CheckpointPath)
	key := sourceCheckpointKey("qdrant", "source", "test", "articles")
	ctx := context.Background()
	if err := store.Save(ctx, key, checkpoint.Checkpoint{SourceDone: true, AcceptedRecords: 1}); err != nil {
		t.Fatal(err)
	}
	err := runMigration(ctx, cfg)
	if err == nil || !strings.Contains(err.Error(), "completed checkpoint has no validation samples") {
		t.Fatalf("error = %v", err)
	}
	if src.reads != 0 {
		t.Fatalf("source reads = %d, want 0", src.reads)
	}
}

func checkpointRunConfig(src source.Source, url, dir string) migrationRunConfig {
	return migrationRunConfig{
		SourceKind: "qdrant", SourceCollection: "source", Source: src,
		LambdaDB:  config.LambdaDBConfig{BaseURL: url, ProjectName: "test", Collection: "articles", APIKey: "test-key"},
		Migration: config.MigrationConfig{BatchSize: 2, MaxBatchBytes: 6000000, WriteMode: config.WriteModeUpsert, CreateCollection: testBoolPtr(false), RetryMaxAttempts: 1, CheckpointPath: dir},
	}
}

type checkpointSource struct {
	records        []source.Record
	emptyFinalPage bool
	reads          int
}

func (*checkpointSource) Name() string                            { return "qdrant" }
func (s *checkpointSource) Count(context.Context) (uint64, error) { return uint64(len(s.records)), nil }
func (s *checkpointSource) Inventory(context.Context) (*source.Inventory, error) {
	return &source.Inventory{CollectionName: "source", RecordCount: uint64(len(s.records))}, nil
}
func (s *checkpointSource) Read(_ context.Context, cursor source.Cursor, _ int) (source.Batch, error) {
	s.reads++
	if cursor.Value == "end" || len(s.records) == 0 {
		return source.Batch{Done: true}, nil
	}
	if cursor.Value != nil {
		return source.Batch{}, fmt.Errorf("unexpected cursor: %v", cursor.Value)
	}
	if s.emptyFinalPage {
		return source.Batch{Records: s.records, NextCursor: &source.Cursor{Value: "end"}}, nil
	}
	return source.Batch{Records: s.records, Done: true}, nil
}
