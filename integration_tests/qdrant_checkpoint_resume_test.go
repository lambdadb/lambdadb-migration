package integration_tests

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	migrationcmd "github.com/lambdadb/lambdadb-migration/cmd"
	"github.com/lambdadb/lambdadb-migration/internal/checkpoint"
	"github.com/lambdadb/lambdadb-migration/internal/config"
)

// A completed migration retains its checkpoint by default. Qdrant returns
// no next cursor on the final page; reruns must neither replay it nor inflate counts.
func TestQdrantToLambdaDBCompletedCheckpointResume(t *testing.T) {
	if !envEnabled("LAMBDADB_MIGRATION_RUN_QDRANT_MOCK_E2E", "LAMBDADB_MIGRATION_RUN_INTEGRATION") {
		t.Skip("set LAMBDADB_MIGRATION_RUN_QDRANT_MOCK_E2E=1 and run local Qdrant")
	}
	qdrantURL := os.Getenv("LAMBDADB_MIGRATION_QDRANT_URL")
	if qdrantURL == "" {
		qdrantURL = "http://localhost:6334"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	collection := fmt.Sprintf("migration_review_resume_%d", time.Now().UnixNano())
	seedQdrantCollection(t, ctx, qdrantURL, collection, largeDenseFixture(3))
	mock := newLambdaDBMock(t, "playground", "articles")
	defer mock.server.Close()
	checkpointDir := t.TempDir()
	cmd := migrationcmd.MigrateQdrantCmd{
		Qdrant:    config.QdrantConfig{URL: qdrantURL, Collection: collection, MaxMessageSize: 32 * 1024 * 1024},
		LambdaDB:  config.LambdaDBConfig{BaseURL: mock.server.URL, ProjectName: "playground", APIKey: "test-key", Collection: "articles"},
		Migration: config.MigrationConfig{BatchSize: 2, MaxBatchBytes: 6_000_000, WriteMode: config.WriteModeUpsert, CreateCollection: boolPtr(true), CheckpointPath: checkpointDir, RetryMaxAttempts: 1, Validate: true, ValidationSampleSize: 3},
	}
	for run := 1; run <= 2; run++ {
		if err := cmd.Run(&migrationcmd.Globals{}); err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		data, err := os.ReadFile(filepath.Join(checkpointDir, "qdrant", collection, "playground", "articles.json"))
		if err != nil {
			t.Fatal(err)
		}
		var cp checkpoint.Checkpoint
		if err := json.Unmarshal(data, &cp); err != nil {
			t.Fatal(err)
		}
		t.Logf("run=%d acceptedRecords=%d cursor=%v totalWritten=%d", run, cp.AcceptedRecords, cp.Cursor, len(mock.docs()))
		if !cp.SourceDone || len(cp.ValidationSamples) != 3 {
			t.Fatalf("missing completion or validation samples: %+v", cp)
		}
		if cp.AcceptedRecords != 3 || len(mock.docs()) != 3 {
			t.Fatalf("completed checkpoint replayed data: acceptedRecords=%d totalWritten=%d, want 3/3", cp.AcceptedRecords, len(mock.docs()))
		}
	}
}
