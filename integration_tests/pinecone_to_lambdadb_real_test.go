package integration_tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	sdk "github.com/lambdadb/go-lambdadb"
	migrationcmd "github.com/lambdadb/lambdadb-migration/cmd"
	"github.com/lambdadb/lambdadb-migration/internal/config"
	pineconeapi "github.com/pinecone-io/go-pinecone/v5/pinecone"
)

func TestPineconeToRealLambdaDBSmoke(t *testing.T) {
	if !envEnabled("LAMBDADB_MIGRATION_RUN_PINECONE_REAL_E2E", "LAMBDADB_MIGRATION_RUN_PINECONE_E2E") {
		t.Skip("set LAMBDADB_MIGRATION_RUN_PINECONE_REAL_E2E=1 with Pinecone and LambdaDB env vars")
	}

	baseURL := os.Getenv("LAMBDADB_BASE_URL")
	projectName := os.Getenv("LAMBDADB_PROJECT_NAME")
	lambdaAPIKey := os.Getenv("LAMBDADB_PROJECT_API_KEY")
	pineconeAPIKey := os.Getenv("PINECONE_API_KEY")
	cloud := os.Getenv("LAMBDADB_MIGRATION_PINECONE_CLOUD")
	if cloud == "" {
		cloud = "aws"
	}
	region := os.Getenv("LAMBDADB_MIGRATION_PINECONE_REGION")
	if region == "" {
		region = "us-east-1"
	}
	if baseURL == "" || projectName == "" || lambdaAPIKey == "" {
		t.Skip("LAMBDADB_BASE_URL, LAMBDADB_PROJECT_NAME, and LAMBDADB_PROJECT_API_KEY are required")
	}
	if pineconeAPIKey == "" {
		t.Skip("PINECONE_API_KEY is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	pineconeClient, err := pineconeapi.NewClient(pineconeapi.NewClientParams{
		ApiKey:    pineconeAPIKey,
		SourceTag: "lambdadb-migration-test",
	})
	if err != nil {
		t.Fatalf("create pinecone client: %v", err)
	}
	lambdaClient := sdk.New(
		sdk.WithBaseURL(baseURL),
		sdk.WithProjectName(projectName),
		sdk.WithAPIKey(lambdaAPIKey),
	)

	suffix := time.Now().UnixNano() % 1_000_000_000
	namespace := os.Getenv("LAMBDADB_MIGRATION_PINECONE_NAMESPACE")

	tests := []struct {
		name      string
		slug      string
		indexSpec pineconeSmokeIndexSpec
		vectors   []*pineconeapi.Vector
		assertDoc func(map[string]any) error
	}{
		{
			name: "dense",
			slug: "dns",
			indexSpec: pineconeSmokeIndexSpec{
				Metric:     pineconeapi.Cosine,
				VectorType: "dense",
				Dimension:  int32(3),
			},
			vectors: pineconeDenseSmokeVectors(t),
			assertDoc: func(doc map[string]any) error {
				if err := requireLambdaDBDocField(doc, "dense"); err != nil {
					return err
				}
				if doc["metadata_url"] != "https://example.com/pinecone-1" {
					return fmt.Errorf("doc = %#v, want normalized metadata_url", doc)
				}
				return nil
			},
		},
		{
			name: "sparse",
			slug: "spr",
			indexSpec: pineconeSmokeIndexSpec{
				Metric:     pineconeapi.Dotproduct,
				VectorType: "sparse",
			},
			vectors: pineconeSparseSmokeVectors(t),
			assertDoc: func(doc map[string]any) error {
				if err := requireLambdaDBDocField(doc, "sparse"); err != nil {
					return err
				}
				if doc["metadata_url"] != "https://example.com/pinecone-1" {
					return fmt.Errorf("doc = %#v, want normalized metadata_url", doc)
				}
				return nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			indexName := fmt.Sprintf("ldb-mig-%s-%d", tt.slug, suffix)
			targetCollection := fmt.Sprintf("mpc_%s_%d", tt.slug, suffix)

			if err := deletePineconeIndexIfExists(ctx, pineconeClient, indexName); err != nil {
				t.Fatalf("delete pre-existing Pinecone index: %v", err)
			}
			if err := deleteLambdaDBCollectionIfExists(ctx, lambdaClient, targetCollection); err != nil {
				t.Fatalf("delete pre-existing LambdaDB collection: %v", err)
			}
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cleanupCancel()
				if err := deletePineconeIndexIfExists(cleanupCtx, pineconeClient, indexName); err != nil {
					t.Logf("delete Pinecone index %q: %v", indexName, err)
				}
				if err := deleteLambdaDBCollectionIfExists(cleanupCtx, lambdaClient, targetCollection); err != nil {
					t.Logf("delete LambdaDB collection %q: %v", targetCollection, err)
				}
			})

			index := createPineconeIndex(t, ctx, pineconeClient, indexName, cloud, region, tt.indexSpec)
			indexConn, err := pineconeClient.Index(pineconeapi.NewIndexConnParams{
				Host:      index.Host,
				Namespace: namespace,
			})
			if err != nil {
				t.Fatalf("connect Pinecone index: %v", err)
			}
			defer indexConn.Close()

			upserted, err := indexConn.UpsertVectors(ctx, tt.vectors)
			if err != nil {
				t.Fatalf("upsert Pinecone vectors: %v", err)
			}
			if upserted != 2 {
				t.Fatalf("upserted %d Pinecone vectors, want 2", upserted)
			}
			if err := waitForPineconeVectorCount(ctx, indexConn, 2); err != nil {
				t.Fatal(err)
			}

			cmd := migrationcmd.MigratePineconeCmd{
				Pinecone: config.PineconeConfig{
					APIKey:    pineconeAPIKey,
					Index:     indexName,
					Namespace: namespace,
				},
				LambdaDB: config.LambdaDBConfig{
					BaseURL:     baseURL,
					ProjectName: projectName,
					APIKey:      lambdaAPIKey,
					Collection:  targetCollection,
				},
				Migration: config.MigrationConfig{
					BatchSize:            2,
					MaxBatchBytes:        6_000_000,
					WriteMode:            config.WriteModeUpsert,
					Restart:              true,
					CreateCollection:     boolPtr(true),
					Validate:             true,
					ValidationSampleSize: 2,
					QueryOverlap:         true,
					QueryOverlapLimit:    2,
					CheckpointPath:       t.TempDir(),
					RetryMaxAttempts:     5,
					RetryInitialDelayMS:  500,
					RetryMaxDelayMS:      5_000,
				},
			}
			if err := cmd.Run(&migrationcmd.Globals{}); err != nil {
				t.Fatalf("migration Run() error = %v", err)
			}

			if err := waitForLambdaDBDocs(ctx, lambdaClient, targetCollection, []string{"pc-1", "pc-2"}, tt.assertDoc); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type pineconeSmokeIndexSpec struct {
	Metric     pineconeapi.IndexMetric
	VectorType string
	Dimension  int32
}

func pineconeDenseSmokeVectors(t *testing.T) []*pineconeapi.Vector {
	t.Helper()
	metadata1 := pineconeSmokeMetadata(t, "Pinecone document 1", "https://example.com/pinecone-1", 1)
	metadata2 := pineconeSmokeMetadata(t, "Pinecone document 2", "https://example.com/pinecone-2", 2)
	values1 := []float32{0.1, 0.2, 0.3}
	values2 := []float32{0.2, 0.1, 0.4}
	return []*pineconeapi.Vector{
		{Id: "pc-1", Values: &values1, Metadata: metadata1},
		{Id: "pc-2", Values: &values2, Metadata: metadata2},
	}
}

func pineconeSparseSmokeVectors(t *testing.T) []*pineconeapi.Vector {
	t.Helper()
	metadata1 := pineconeSmokeMetadata(t, "Pinecone sparse document 1", "https://example.com/pinecone-1", 1)
	metadata2 := pineconeSmokeMetadata(t, "Pinecone sparse document 2", "https://example.com/pinecone-2", 2)
	return []*pineconeapi.Vector{
		{
			Id:           "pc-1",
			SparseValues: &pineconeapi.SparseValues{Indices: []uint32{3, 9}, Values: []float32{0.7, 0.2}},
			Metadata:     metadata1,
		},
		{
			Id:           "pc-2",
			SparseValues: &pineconeapi.SparseValues{Indices: []uint32{1, 8}, Values: []float32{0.4, 0.9}},
			Metadata:     metadata2,
		},
	}
}

func pineconeSmokeMetadata(t *testing.T, title, url string, rank float64) *pineconeapi.Metadata {
	t.Helper()
	metadata, err := pineconeapi.NewMetadata(map[string]any{
		"title":        title,
		"metadata.url": url,
		"rank":         rank,
	})
	if err != nil {
		t.Fatalf("create Pinecone metadata: %v", err)
	}
	return metadata
}

func createPineconeIndex(t *testing.T, ctx context.Context, client *pineconeapi.Client, name, cloud, region string, spec pineconeSmokeIndexSpec) *pineconeapi.Index {
	t.Helper()
	deletionProtection := pineconeapi.DeletionProtectionDisabled
	req := &pineconeapi.CreateServerlessIndexRequest{
		Name:               name,
		Cloud:              pineconeapi.Cloud(cloud),
		Region:             region,
		Metric:             &spec.Metric,
		VectorType:         &spec.VectorType,
		DeletionProtection: &deletionProtection,
	}
	if spec.Dimension > 0 {
		req.Dimension = &spec.Dimension
	}
	index, err := client.CreateServerlessIndex(ctx, req)
	if err != nil {
		t.Fatalf("create Pinecone index: %v", err)
	}
	return waitForPineconeIndexReady(t, ctx, client, index.Name)
}

func waitForPineconeIndexReady(t *testing.T, ctx context.Context, client *pineconeapi.Client, name string) *pineconeapi.Index {
	t.Helper()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		index, err := client.DescribeIndex(ctx, name)
		if err == nil && index != nil && index.Status != nil && index.Status.Ready {
			return index
		}

		select {
		case <-ctx.Done():
			if err != nil {
				t.Fatalf("wait for Pinecone index %q: last error: %v", name, err)
			}
			t.Fatalf("wait for Pinecone index %q: %v", name, ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitForPineconeVectorCount(ctx context.Context, conn *pineconeapi.IndexConnection, want uint64) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastCount uint64
	for {
		stats, err := conn.DescribeIndexStats(ctx)
		if err == nil && stats != nil {
			lastCount = uint64(stats.TotalVectorCount)
			if lastCount >= want {
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for Pinecone vector count %d: last count %d: %w", want, lastCount, ctx.Err())
		case <-ticker.C:
		}
	}
}

func deletePineconeIndexIfExists(ctx context.Context, client *pineconeapi.Client, name string) error {
	err := client.DeleteIndex(ctx, name)
	if err == nil {
		return waitForPineconeIndexDeleted(ctx, client, name)
	}
	if isPineconeNotFound(err) {
		return nil
	}
	return err
}

func waitForPineconeIndexDeleted(ctx context.Context, client *pineconeapi.Client, name string) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		_, err := client.DescribeIndex(ctx, name)
		if isPineconeNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for Pinecone index %q delete: %w", name, ctx.Err())
		case <-ticker.C:
		}
	}
}

func isPineconeNotFound(err error) bool {
	if err == nil {
		return false
	}
	var pineconeErr *pineconeapi.PineconeError
	if errors.As(err, &pineconeErr) && pineconeErr.Code == 404 {
		return true
	}
	return strings.Contains(err.Error(), "NOT_FOUND") || strings.Contains(err.Error(), "not found")
}
