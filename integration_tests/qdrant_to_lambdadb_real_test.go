package integration_tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	sdk "github.com/lambdadb/go-lambdadb"
	"github.com/lambdadb/go-lambdadb/models/apierrors"
	migrationcmd "github.com/lambdadb/lambdadb-migration/cmd"
	"github.com/lambdadb/lambdadb-migration/internal/config"
)

func TestQdrantToRealLambdaDBSmoke(t *testing.T) {
	if os.Getenv("LAMBDADB_MIGRATION_RUN_REAL_E2E") != "1" {
		t.Skip("set LAMBDADB_MIGRATION_RUN_REAL_E2E=1 with LambdaDB env vars and run Qdrant from integration_tests/compose/qdrant.yaml")
	}

	baseURL := os.Getenv("LAMBDADB_BASE_URL")
	projectName := os.Getenv("LAMBDADB_PROJECT_NAME")
	apiKey := os.Getenv("LAMBDADB_PROJECT_API_KEY")
	if baseURL == "" || projectName == "" || apiKey == "" {
		t.Skip("LAMBDADB_BASE_URL, LAMBDADB_PROJECT_NAME, and LAMBDADB_PROJECT_API_KEY are required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	qdrantURL := os.Getenv("LAMBDADB_MIGRATION_QDRANT_URL")
	if qdrantURL == "" {
		qdrantURL = "http://localhost:6334"
	}
	lambdaClient := sdk.New(
		sdk.WithBaseURL(baseURL),
		sdk.WithProjectName(projectName),
		sdk.WithAPIKey(apiKey),
	)
	largeCount := 64
	if raw := os.Getenv("LAMBDADB_MIGRATION_REAL_LARGE_COUNT"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			t.Fatalf("LAMBDADB_MIGRATION_REAL_LARGE_COUNT=%q, want positive integer", raw)
		}
		largeCount = parsed
	}

	tests := []struct {
		name      string
		slug      string
		fixture   qdrantFixture
		writeMode config.WriteMode
		batchSize int
		mapping   func(string) config.MappingConfig
		ids       []string
		assertDoc func(map[string]any) error
	}{
		{
			name:      "unnamed_dense_upsert",
			slug:      "udu",
			fixture:   unnamedDenseFixture(),
			writeMode: config.WriteModeUpsert,
			batchSize: 2,
			ids:       []string{"1", "2"},
			assertDoc: func(doc map[string]any) error {
				if err := requireLambdaDBDocField(doc, "dense"); err != nil {
					return err
				}
				if doc["metadata_url"] != "https://example.com/1" {
					return fmt.Errorf("doc = %#v, want normalized metadata_url", doc)
				}
				return nil
			},
		},
		{
			name:      "named_dense_upsert",
			slug:      "ndu",
			fixture:   namedDenseFixture(),
			writeMode: config.WriteModeUpsert,
			batchSize: 2,
			ids:       []string{"101", "102"},
			assertDoc: func(doc map[string]any) error {
				if err := requireLambdaDBDocField(doc, "title_dense"); err != nil {
					return err
				}
				return requireLambdaDBDocField(doc, "body_dense")
			},
		},
		{
			name:      "dense_sparse_payload_indexes_upsert",
			slug:      "dspu",
			fixture:   denseSparsePayloadIndexFixture(),
			writeMode: config.WriteModeUpsert,
			batchSize: 2,
			ids:       []string{"201", "202"},
			assertDoc: func(doc map[string]any) error {
				if err := requireLambdaDBDocField(doc, "body_dense"); err != nil {
					return err
				}
				if err := requireLambdaDBDocField(doc, "keywords_sparse"); err != nil {
					return err
				}
				if doc["category"] != "docs" || doc["views"] == nil {
					return fmt.Errorf("doc = %#v, want indexed payload fields", doc)
				}
				return nil
			},
		},
		{
			name:      "unnamed_dense_bulk",
			slug:      "udb",
			fixture:   unnamedDenseFixture(),
			writeMode: config.WriteModeBulk,
			batchSize: 2,
			ids:       []string{"1", "2"},
			assertDoc: func(doc map[string]any) error {
				if err := requireLambdaDBDocField(doc, "dense"); err != nil {
					return err
				}
				if doc["metadata_url"] != "https://example.com/1" {
					return fmt.Errorf("doc = %#v, want normalized metadata_url", doc)
				}
				return nil
			},
		},
		{
			name:      "additional_payload_index_types_upsert",
			slug:      "apitu",
			fixture:   additionalPayloadIndexTypesFixture(),
			writeMode: config.WriteModeUpsert,
			batchSize: 2,
			mapping:   additionalPayloadIndexTypesMapping,
			ids:       []string{"501", "502"},
			assertDoc: func(doc map[string]any) error {
				for _, field := range []string{"dense", "body", "score", "published_at", "is_public", "attributes"} {
					if err := requireLambdaDBDocField(doc, field); err != nil {
						return err
					}
				}
				return nil
			},
		},
		{
			name:      "larger_dense_bulk",
			slug:      "ldb",
			fixture:   largeDenseFixture(largeCount),
			writeMode: config.WriteModeBulk,
			batchSize: 17,
			ids:       largeDenseIDs(largeCount),
			assertDoc: func(doc map[string]any) error {
				if err := requireLambdaDBDocField(doc, "dense"); err != nil {
					return err
				}
				if err := requireLambdaDBDocField(doc, "rank"); err != nil {
					return err
				}
				return requireLambdaDBDocField(doc, "title")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			suffix := time.Now().UnixNano()
			sourceCollection := fmt.Sprintf("lambdadb_migration_real_source_%s_%d", tt.slug, suffix)
			targetCollection := fmt.Sprintf("mre_%s_%d", tt.slug, suffix%1_000_000_000)

			seedQdrantCollection(t, ctx, qdrantURL, sourceCollection, tt.fixture)
			var mappingFile string
			if tt.mapping != nil {
				mappingFile = writeMappingFile(t, tt.mapping(targetCollection))
			}

			if err := deleteLambdaDBCollectionIfExists(ctx, lambdaClient, targetCollection); err != nil {
				t.Fatalf("delete pre-existing LambdaDB collection: %v", err)
			}
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cleanupCancel()
				if err := deleteLambdaDBCollectionIfExists(cleanupCtx, lambdaClient, targetCollection); err != nil {
					t.Logf("delete LambdaDB collection %q: %v", targetCollection, err)
				}
			})

			cmd := migrationcmd.MigrateQdrantCmd{
				Qdrant: config.QdrantConfig{
					URL:            qdrantURL,
					Collection:     sourceCollection,
					MaxMessageSize: 32 * 1024 * 1024,
				},
				LambdaDB: config.LambdaDBConfig{
					BaseURL:     baseURL,
					ProjectName: projectName,
					APIKey:      apiKey,
					Collection:  targetCollection,
				},
				Migration: config.MigrationConfig{
					BatchSize:            tt.batchSize,
					MaxBatchBytes:        6_000_000,
					WriteMode:            tt.writeMode,
					Restart:              true,
					CreateCollection:     true,
					Validate:             true,
					ValidationSampleSize: 10,
					CheckpointPath:       t.TempDir(),
					RetryMaxAttempts:     5,
					RetryInitialDelayMS:  500,
					RetryMaxDelayMS:      5_000,
				},
				MappingFile: mappingFile,
			}
			if err := cmd.Run(&migrationcmd.Globals{}); err != nil {
				t.Fatalf("migration Run() error = %v", err)
			}

			if err := waitForLambdaDBDocs(ctx, lambdaClient, targetCollection, tt.ids, tt.assertDoc); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func waitForLambdaDBDocs(ctx context.Context, client *sdk.Client, collection string, ids []string, assertDoc func(map[string]any) error) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var lastTotal int64
	var lastDocs int
	for {
		result, err := client.Collection(collection).Docs().Fetch(ctx, sdk.FetchDocsInput{
			Ids:            ids,
			ConsistentRead: sdk.Bool(true),
			IncludeVectors: sdk.Bool(true),
		})
		if err == nil && result != nil {
			lastTotal = result.Total
			lastDocs = len(result.Docs)
			if len(result.Docs) >= len(ids) {
				for _, doc := range result.Docs {
					if doc.Doc["id"] == ids[0] {
						if assertDoc != nil {
							if err := assertDoc(doc.Doc); err != nil {
								return err
							}
						}
						return nil
					}
				}
				return fmt.Errorf("fetched %d LambdaDB docs but none matched ID %q: %#v", len(result.Docs), ids[0], result.Docs)
			}
		}

		select {
		case <-ctx.Done():
			if err != nil {
				return fmt.Errorf("wait for LambdaDB docs %v: last error: %w", ids, err)
			}
			return fmt.Errorf("wait for LambdaDB docs %v: last total %d, last docs %d: %w", ids, lastTotal, lastDocs, ctx.Err())
		case <-ticker.C:
		}
	}
}

func largeDenseIDs(count int) []string {
	ids := make([]string, 0, count)
	for i := 1; i <= count; i++ {
		ids = append(ids, strconv.Itoa(1000+i))
	}
	return ids
}

func deleteLambdaDBCollectionIfExists(ctx context.Context, client *sdk.Client, collection string) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		_, err := client.Collection(collection).Delete(ctx)
		if err == nil {
			return nil
		}
		var notFound *apierrors.ResourceNotFoundError
		if errors.As(err, &notFound) {
			return nil
		}
		if !strings.Contains(err.Error(), "CREATING state") && !strings.Contains(err.Error(), "DELETING state") {
			return err
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("delete LambdaDB collection %q: %w", collection, ctx.Err())
		case <-ticker.C:
		}
	}
}

func requireLambdaDBDocField(doc map[string]any, field string) error {
	if _, ok := doc[field]; !ok {
		return fmt.Errorf("doc = %#v, want field %q", doc, field)
	}
	return nil
}
