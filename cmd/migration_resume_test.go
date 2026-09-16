package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/lambdadb/lambdadb-migration/internal/checkpoint"
	"github.com/lambdadb/lambdadb-migration/internal/config"
	"github.com/lambdadb/lambdadb-migration/internal/source"
)

// A source page can become multiple target writes. If a later write fails,
// resume must replay that page and advance the checkpoint only after all writes.
func TestMigrationCheckpointResume(t *testing.T) {
	for _, tt := range []struct {
		name         string
		mode         config.WriteMode
		failureStage string
	}{
		{"upsert", config.WriteModeUpsert, "write"},
		{"bulk_upload", config.WriteModeBulk, "upload"},
		{"bulk_completion", config.WriteModeBulk, "write"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var mu sync.Mutex
			var attempts []string
			objects := map[string][]map[string]any{}
			nextObject := 0
			fail := true
			var serverURL string
			const base = "/projects/test/collections/articles"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == "GET" && r.URL.Path == base:
					fmt.Fprint(w, `{"collection":{"collectionName":"articles","defaultBranchName":"main","numDocs":0}}`)
				case r.Method == "GET" && r.URL.Path == base+"/docs/bulk-upsert":
					nextObject++
					key := fmt.Sprintf("object-%d", nextObject)
					json.NewEncoder(w).Encode(map[string]any{
						"url": serverURL + "/upload/" + key, "objectKey": key,
						"httpMethod": "PUT", "type": "application/json", "sizeLimitBytes": 200000000,
						"headers": map[string]string{"if-none-match": "*"},
					})
				case r.Method == "PUT" && strings.HasPrefix(r.URL.Path, "/upload/"):
					var body struct {
						Docs []map[string]any `json:"docs"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Docs) != 1 {
						t.Errorf("expected one document per upload, got %+v: %v", body, err)
						w.WriteHeader(400)
						return
					}
					id, _ := body.Docs[0]["id"].(string)
					if tt.failureStage == "upload" {
						attempts = append(attempts, id)
						if fail && id == "3" {
							w.WriteHeader(400)
							return
						}
					}
					objects[strings.TrimPrefix(r.URL.Path, "/upload/")] = body.Docs
				case r.Method == "POST" && (r.URL.Path == base+"/docs/upsert" || r.URL.Path == base+"/docs/bulk-upsert"):
					var body struct {
						Docs      []map[string]any `json:"docs"`
						ObjectKey string           `json:"objectKey"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Errorf("decode write: %v", err)
					}
					if tt.mode == config.WriteModeBulk {
						body.Docs = objects[body.ObjectKey]
					}
					if len(body.Docs) != 1 {
						t.Errorf("expected one document per write, got %+v", body)
						w.WriteHeader(400)
						return
					}
					id, _ := body.Docs[0]["id"].(string)
					if tt.failureStage == "write" {
						attempts = append(attempts, id)
						if fail && id == "3" {
							w.WriteHeader(400)
							fmt.Fprint(w, `{"message":"injected write failure"}`)
							return
						}
					}
					w.WriteHeader(http.StatusAccepted)
					fmt.Fprint(w, `{"message":"accepted"}`)
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			serverURL = server.URL
			src := &resumeSource{}
			cfg := migrationRunConfig{
				SourceKind: "qdrant", SourceCollection: "source", Source: src,
				LambdaDB:  config.LambdaDBConfig{BaseURL: server.URL, ProjectName: "test", Collection: "articles", APIKey: "test-key"},
				Migration: config.MigrationConfig{BatchSize: 2, MaxBatchBytes: 25, WriteMode: tt.mode, RetryMaxAttempts: 1, CheckpointPath: t.TempDir()},
			}
			ctx := context.Background()
			store := checkpoint.NewFileStore(cfg.Migration.CheckpointPath)
			key := sourceCheckpointKey("qdrant", "source", "test", "articles")
			assertCheckpoint := func(accepted uint64, cursor string) {
				t.Helper()
				cp, err := store.Load(ctx, key)
				if err != nil || cp == nil {
					t.Fatalf("Load() = %+v, %v", cp, err)
				}
				if cp.AcceptedRecords != accepted || cp.Cursor != cursor || cp.SourceKind != "qdrant" || cp.SourceCollection != "source" || cp.TargetCollection != "articles" {
					t.Fatalf("checkpoint = %+v, want accepted=%d cursor=%s", cp, accepted, cursor)
				}
			}
			if err := runMigration(ctx, cfg); err == nil {
				t.Fatal("expected injected failure")
			}
			assertCheckpoint(1, "page2")
			mu.Lock()
			fail = false
			mu.Unlock()
			src.cursors = nil
			if err := runMigration(ctx, cfg); err != nil {
				t.Fatalf("resume: %v", err)
			}
			assertCheckpoint(3, "done")
			if !reflect.DeepEqual(src.cursors, []any{"page2"}) {
				t.Fatalf("resume cursors = %v", src.cursors)
			}
			mu.Lock()
			gotAttempts := append([]string(nil), attempts...)
			mu.Unlock()
			if !reflect.DeepEqual(gotAttempts, []string{"1", "2", "3", "2", "3"}) {
				t.Fatalf("attempts = %v, want failed page replay only", gotAttempts)
			}

			// Explicit restart still ignores the saved cursor and accepted count.
			cfg.Migration.Restart = true
			src.cursors = nil
			if err := runMigration(ctx, cfg); err != nil {
				t.Fatalf("restart: %v", err)
			}
			assertCheckpoint(3, "done")
			if !reflect.DeepEqual(src.cursors, []any{nil, "page2"}) {
				t.Fatalf("restart cursors = %v", src.cursors)
			}

			// A completed checkpoint resumes without writing; cleanup stays opt-in.
			cfg.Migration.Restart = false
			cfg.Migration.CleanupCheckpoint = true
			src.cursors = nil
			if err := runMigration(ctx, cfg); err != nil {
				t.Fatalf("cleanup: %v", err)
			}
			if !reflect.DeepEqual(src.cursors, []any{"done"}) {
				t.Fatalf("completed cursors = %v", src.cursors)
			}
			if cp, err := store.Load(ctx, key); err != nil || cp != nil {
				t.Fatalf("checkpoint after cleanup = %+v, %v", cp, err)
			}
			mu.Lock()
			defer mu.Unlock()
			if !reflect.DeepEqual(attempts, []string{"1", "2", "3", "2", "3", "1", "2", "3"}) {
				t.Fatalf("final attempts = %v", attempts)
			}
		})
	}
}

type resumeSource struct{ cursors []any }

func (*resumeSource) Name() string                          { return "qdrant" }
func (*resumeSource) Count(context.Context) (uint64, error) { return 3, nil }
func (*resumeSource) Inventory(context.Context) (*source.Inventory, error) {
	return &source.Inventory{CollectionName: "source", RecordCount: 3}, nil
}
func (s *resumeSource) Read(_ context.Context, cursor source.Cursor, _ int) (source.Batch, error) {
	s.cursors = append(s.cursors, cursor.Value)
	switch cursor.Value {
	case nil:
		return source.Batch{Records: []source.Record{{ID: "1"}}, NextCursor: &source.Cursor{Value: "page2"}}, nil
	case "page2":
		return source.Batch{Records: []source.Record{{ID: "2"}, {ID: "3"}}, NextCursor: &source.Cursor{Value: "done"}, Done: true}, nil
	case "done":
		return source.Batch{Done: true}, nil
	default:
		return source.Batch{}, fmt.Errorf("unexpected cursor: %v", cursor.Value)
	}
}
