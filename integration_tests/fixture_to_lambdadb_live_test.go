package integration_tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	sdk "github.com/lambdadb/go-lambdadb"
	"github.com/lambdadb/go-lambdadb/models/apierrors"
	"github.com/lambdadb/go-lambdadb/models/components"
	migrationcmd "github.com/lambdadb/lambdadb-migration/cmd"
	"github.com/lambdadb/lambdadb-migration/internal/config"
	"github.com/lambdadb/lambdadb-migration/internal/source"
)

const fixtureLiveFlag = "LAMBDADB_MIGRATION_RUN_FIXTURE_LIVE_E2E"

// TestFixtureSourcesToLambdaDBLive migrates Qdrant-, Pinecone-, and
// Elasticsearch-shaped fixtures through the CLI migration pipeline into a real
// LambdaDB project. It verifies each collection's schema, document count, and
// document content, then deletes the collection and verifies its absence.
// Cases with wantErr must fail mapping validation without creating a collection.
func TestFixtureSourcesToLambdaDBLive(t *testing.T) {
	if !envEnabled(fixtureLiveFlag) {
		t.Skip("set " + fixtureLiveFlag + "=1 with LAMBDADB_BASE_URL, LAMBDADB_PROJECT_NAME, and LAMBDADB_PROJECT_API_KEY")
	}
	baseURL := os.Getenv("LAMBDADB_BASE_URL")
	projectName := os.Getenv("LAMBDADB_PROJECT_NAME")
	apiKey := os.Getenv("LAMBDADB_PROJECT_API_KEY")
	if baseURL == "" || projectName == "" || apiKey == "" {
		// An enabled live run must not pass without exercising LambdaDB.
		t.Fatal(fixtureLiveFlag + "=1 requires LAMBDADB_BASE_URL, LAMBDADB_PROJECT_NAME, and LAMBDADB_PROJECT_API_KEY")
	}
	client := sdk.New(sdk.WithBaseURL(baseURL), sdk.WithProjectName(projectName), sdk.WithAPIKey(apiKey))
	suffix := time.Now().UnixNano() % 1_000_000_000
	const largeCount = 64

	vector := func(similarity string) map[string]any {
		return map[string]any{"type": "vector", "dimensions": float64(fixtureVectorDims), "similarity": similarity}
	}
	indexType := func(typ string) map[string]any { return map[string]any{"type": typ} }
	unnamedDenseDocs := map[string]map[string]any{
		"1": {"id": "1", "title": "one", "metadata_url": "https://example.com/1", "dense": unit(0.1, 0.2, 0.3)},
		"2": {"id": "2", "title": "two", "metadata_url": "https://example.com/2", "dense": unit(0.4, 0.5, 0.6)},
	}

	tests := []struct {
		name         string
		slug         string
		source       *fixtureSource
		writeMode    config.WriteMode
		batchSize    int
		mapping      func(string) config.MappingConfig
		queryOverlap bool
		wantIndexes  map[string]map[string]any
		wantDocs     map[string]map[string]any
		wantErr      string
	}{
		{
			name:         "qdrant_unnamed_dense_upsert",
			slug:         "qudu",
			source:       qdrantFixtureSource("unnamed_dense", unnamedDenseFixture()),
			writeMode:    config.WriteModeUpsert,
			batchSize:    2,
			queryOverlap: true,
			wantIndexes:  map[string]map[string]any{"dense": vector("cosine")},
			wantDocs:     unnamedDenseDocs,
		},
		{
			name:      "qdrant_named_dense_upsert",
			slug:      "qndu",
			source:    qdrantFixtureSource("named_dense", namedDenseFixture()),
			writeMode: config.WriteModeUpsert,
			batchSize: 2,
			wantIndexes: map[string]map[string]any{
				"title_dense": vector("cosine"),
				"body_dense":  vector("euclidean"),
			},
			wantDocs: map[string]map[string]any{
				"101": {"id": "101", "title": "named one", "title_dense": unit(0.1, 0.2), "body_dense": padded(0.1, 0.2, 0.3, 0.4)},
				"102": {"id": "102", "title": "named two", "title_dense": unit(0.3, 0.4), "body_dense": padded(0.5, 0.6, 0.7, 0.8)},
			},
		},
		{
			name:         "qdrant_dense_sparse_payload_indexes_upsert",
			slug:         "qdsp",
			source:       qdrantFixtureSource("dense_sparse_payload", denseSparsePayloadIndexFixture()),
			writeMode:    config.WriteModeUpsert,
			batchSize:    2,
			queryOverlap: true,
			wantIndexes: map[string]map[string]any{
				"body_dense":      vector("cosine"),
				"keywords_sparse": indexType("sparseVector"),
				"category":        indexType("keyword"),
				"views":           indexType("long"),
			},
			wantDocs: map[string]map[string]any{
				"201": {"id": "201", "category": "docs", "views": 12.0, "body_dense": unit(0.2, 0.3, 0.4), "keywords_sparse": map[string]any{"3": 0.7, "9": 0.2}},
				"202": {"id": "202", "category": "guides", "views": 34.0, "body_dense": unit(0.5, 0.6, 0.7), "keywords_sparse": map[string]any{"1": 0.4, "8": 0.9}},
			},
		},
		{
			name:      "qdrant_additional_payload_index_types_upsert",
			slug:      "qapi",
			source:    qdrantFixtureSource("additional_payload_index_types", additionalPayloadIndexTypesFixture()),
			writeMode: config.WriteModeUpsert,
			batchSize: 2,
			mapping:   additionalPayloadIndexTypesMapping,
			wantIndexes: map[string]map[string]any{
				"dense":        vector("cosine"),
				"body":         {"type": "text", "analyzers": []any{"standard"}},
				"score":        indexType("double"),
				"published_at": indexType("datetime"),
				"is_public":    indexType("boolean"),
				"attributes":   indexType("object"),
			},
			wantDocs: map[string]map[string]any{
				"501": {"id": "501", "dense": unit(0.1, 0.2, 0.3), "body": "first searchable body", "score": 0.95, "published_at": "2026-05-10T00:00:00Z", "is_public": true, "attributes": map[string]any{"section": "docs", "rank": 1.0}},
				"502": {"id": "502", "dense": unit(0.4, 0.5, 0.6), "body": "second searchable body", "score": 0.87, "published_at": "2026-05-11T00:00:00Z", "is_public": false, "attributes": map[string]any{"section": "guides", "rank": 2.0}},
			},
		},
		{
			// Native embeddings from ordered sources: the server calls Cohere for
			// each upserted document, so only the generated vector shape is checked.
			name:      "qdrant_cohere_native_sources_upsert",
			slug:      "qcoh",
			source:    qdrantFixtureSource("additional_payload_index_types", additionalPayloadIndexTypesFixture()),
			writeMode: config.WriteModeUpsert,
			batchSize: 2,
			mapping:   cohereNativeSourcesMapping,
			wantIndexes: map[string]map[string]any{
				"dense":        vector("cosine"),
				"body":         {"type": "text", "analyzers": []any{"standard"}},
				"score":        indexType("double"),
				"published_at": indexType("datetime"),
				"is_public":    indexType("boolean"),
				"attributes":   indexType("object"),
				"generated": {"type": "vector", "managedEmbedding": true, "embedding": map[string]any{
					"provider": "cohere", "model": "embed-v5.0-fast", "dimensions": float64(cohereFixtureDims), "similarity": "cosine",
					"sources": []any{map[string]any{"field": "body", "type": "text"}, map[string]any{"field": "attributes.section", "type": "text"}},
				}},
			},
			wantDocs: map[string]map[string]any{
				"501": {"id": "501", "dense": unit(0.1, 0.2, 0.3), "body": "first searchable body", "score": 0.95, "published_at": "2026-05-10T00:00:00Z", "is_public": true, "attributes": map[string]any{"section": "docs", "rank": 1.0}, "generated": generatedVector(cohereFixtureDims)},
				"502": {"id": "502", "dense": unit(0.4, 0.5, 0.6), "body": "second searchable body", "score": 0.87, "published_at": "2026-05-11T00:00:00Z", "is_public": false, "attributes": map[string]any{"section": "guides", "rank": 2.0}, "generated": generatedVector(cohereFixtureDims)},
			},
		},
		{
			name:        "qdrant_unnamed_dense_bulk",
			slug:        "qudb",
			source:      qdrantFixtureSource("unnamed_dense", unnamedDenseFixture()),
			writeMode:   config.WriteModeBulk,
			batchSize:   2,
			wantIndexes: map[string]map[string]any{"dense": vector("cosine")},
			wantDocs:    unnamedDenseDocs,
		},
		{
			name:        "qdrant_large_dense_bulk",
			slug:        "qldb",
			source:      qdrantFixtureSource("large_dense", largeDenseFixture(largeCount)),
			writeMode:   config.WriteModeBulk,
			batchSize:   17,
			wantIndexes: map[string]map[string]any{"dense": vector("cosine"), "rank": indexType("long")},
			wantDocs:    largeDenseDocs(largeCount),
		},
		{
			name:         "pinecone_dense_upsert",
			slug:         "pdns",
			source:       pineconeFixtureSource(t, "dense", pineconeDenseSmokeVectors(t), &source.VectorField{Name: "dense", Dimensions: fixtureVectorDims, Similarity: "cosine", SourceSimilarity: "cosine"}),
			writeMode:    config.WriteModeUpsert,
			batchSize:    2,
			queryOverlap: true,
			wantIndexes:  map[string]map[string]any{"dense": vector("cosine")},
			wantDocs: map[string]map[string]any{
				"pc-1": {"id": "pc-1", "title": "Pinecone document 1", "metadata_url": "https://example.com/pinecone-1", "rank": 1.0, "dense": padded(0.1, 0.2, 0.3)},
				"pc-2": {"id": "pc-2", "title": "Pinecone document 2", "metadata_url": "https://example.com/pinecone-2", "rank": 2.0, "dense": padded(0.2, 0.1, 0.4)},
			},
		},
		{
			name:      "pinecone_dotproduct_rejected",
			slug:      "pdot",
			source:    pineconeFixtureSource(t, "dotproduct", pineconeDenseSmokeVectors(t), &source.VectorField{Name: "dense", Dimensions: fixtureVectorDims, Similarity: "dot_product", SourceSimilarity: "dotproduct"}),
			writeMode: config.WriteModeUpsert,
			batchSize: 2,
			wantErr:   `uses unsupported similarity "dot_product" (source pinecone metric "dotproduct"); LambdaDB supports cosine, euclidean`,
		},
		{
			name:         "pinecone_sparse_upsert",
			slug:         "pspr",
			source:       pineconeFixtureSource(t, "sparse", pineconeSparseSmokeVectors(t), nil),
			writeMode:    config.WriteModeUpsert,
			batchSize:    2,
			queryOverlap: true,
			wantIndexes:  map[string]map[string]any{"sparse": indexType("sparseVector")},
			wantDocs: map[string]map[string]any{
				"pc-1": {"id": "pc-1", "title": "Pinecone sparse document 1", "metadata_url": "https://example.com/pinecone-1", "rank": 1.0, "sparse": map[string]any{"3": 0.7, "9": 0.2}},
				"pc-2": {"id": "pc-2", "title": "Pinecone sparse document 2", "metadata_url": "https://example.com/pinecone-2", "rank": 2.0, "sparse": map[string]any{"1": 0.4, "8": 0.9}},
			},
		},
		{
			name:      "elasticsearch_text_analyzers_upsert",
			slug:      "etxt",
			source:    elasticsearchTextAnalyzerFixture("articles"),
			writeMode: config.WriteModeUpsert,
			batchSize: 1,
			wantIndexes: map[string]map[string]any{
				"embedding":    vector("euclidean"),
				"title":        {"type": "text", "analyzers": []any{"english"}},
				"body":         {"type": "text", "analyzers": []any{"standard"}},
				"notes":        indexType("text"),
				"tags":         indexType("keyword"),
				"stats_views":  indexType("long"),
				"stats_rating": indexType("double"),
				"published_at": indexType("datetime"),
				"is_public":    indexType("boolean"),
			},
			wantDocs: map[string]map[string]any{
				"es-1": {"id": "es-1", "title": "Migrating search indexes", "body": "Analyzers map to LambdaDB presets", "notes": "server default analyzer", "tags": "search", "stats_views": 12.0, "stats_rating": 4.5, "published_at": "2026-05-10T00:00:00Z", "is_public": true, "embedding": padded(0.1, 0.2, 0.3)},
				"es-2": {"id": "es-2", "title": "Running queries", "body": "Standard tokenization keeps words", "notes": "second note", "tags": "query", "stats_views": 34.0, "stats_rating": 3.25, "published_at": "2026-05-11T00:00:00Z", "is_public": false, "embedding": padded(0.4, 0.5, 0.6)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			collection := fmt.Sprintf("mfx_%s_%d", tt.slug, suffix)
			if err := deleteLambdaDBCollectionIfExists(ctx, client, collection); err != nil {
				t.Fatalf("delete pre-existing LambdaDB collection: %v", err)
			}
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cleanupCancel()
				if err := deleteLambdaDBCollectionIfExists(cleanupCtx, client, collection); err != nil {
					t.Errorf("delete LambdaDB collection %q: %v", collection, err)
					return
				}
				if err := waitForLambdaDBCollectionAbsent(cleanupCtx, client, collection); err != nil {
					t.Error(err)
				}
			})

			var mappingFile string
			if tt.mapping != nil {
				mappingFile = writeMappingFile(t, tt.mapping(collection))
			}
			reportPath := filepath.Join(t.TempDir(), "validation.json")
			err := migrationcmd.RunMigration(ctx, migrationcmd.MigrationRun{
				SourceKind:       tt.source.Name(),
				SourceCollection: tt.source.inventory.CollectionName,
				Source:           tt.source,
				LambdaDB:         config.LambdaDBConfig{BaseURL: baseURL, ProjectName: projectName, APIKey: apiKey, Collection: collection},
				Migration: config.MigrationConfig{
					BatchSize:            tt.batchSize,
					MaxBatchBytes:        6_000_000,
					WriteMode:            tt.writeMode,
					Restart:              true,
					CreateCollection:     boolPtr(true),
					Validate:             true,
					ValidationSampleSize: 10,
					ValidationReport:     reportPath,
					QueryOverlap:         tt.queryOverlap,
					QueryOverlapLimit:    2,
					CheckpointPath:       t.TempDir(),
					RetryMaxAttempts:     5,
					RetryInitialDelayMS:  500,
					RetryMaxDelayMS:      5_000,
				},
				MappingFile: mappingFile,
			})
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("RunMigration() error = %v, want it to contain %q", err, tt.wantErr)
				}
				_, err := client.Collection(collection).Get(ctx)
				var notFound *apierrors.ResourceNotFoundError
				if !errors.As(err, &notFound) {
					t.Fatalf("get LambdaDB collection after rejected migration: error = %v, want not found", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("RunMigration() error = %v", err)
			}
			requireValidationReport(t, reportPath, len(tt.wantDocs), tt.queryOverlap)

			got, err := client.Collection(collection).Get(ctx)
			if err != nil {
				t.Fatalf("get LambdaDB collection: %v", err)
			}
			if err := compareIndexConfigs(got.IndexConfigs, tt.wantIndexes); err != nil {
				t.Fatal(err)
			}
			if err := compareLambdaDBDocs(ctx, client, collection, tt.wantDocs); err != nil {
				t.Fatal(err)
			}
			if err := waitForLambdaDBNumDocs(ctx, client, collection, int64(len(tt.wantDocs))); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func requireValidationReport(t *testing.T, path string, records int, queryOverlap bool) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read validation report: %v", err)
	}
	var report struct {
		Status          string `json:"status"`
		SourceCount     int    `json:"sourceCount"`
		AcceptedRecords int    `json:"acceptedRecords"`
		Samples         struct {
			Compared int `json:"compared"`
		} `json:"samples"`
		QueryOverlap *struct {
			Compared int `json:"compared"`
		} `json:"queryOverlap"`
	}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("decode validation report: %v", err)
	}
	if report.Status != "pass" || report.SourceCount != records || report.AcceptedRecords != records || report.Samples.Compared != min(records, 10) {
		t.Fatalf("validation report = %s, want pass for %d records", data, records)
	}
	if queryOverlap && (report.QueryOverlap == nil || report.QueryOverlap.Compared == 0) {
		t.Fatalf("validation report = %s, want query overlap comparisons", data)
	}
}

// compareIndexConfigs requires exactly the expected index fields plus the
// server-managed id keyword, each with the expected subset of its configuration.
func compareIndexConfigs(got map[string]components.IndexConfigsUnion, wantFields map[string]map[string]any) error {
	want := map[string]map[string]any{"id": {"type": "keyword"}}
	for field, config := range wantFields {
		want[field] = config
	}
	data, err := json.Marshal(got)
	if err != nil {
		return fmt.Errorf("encode LambdaDB index configs: %w", err)
	}
	var actual map[string]map[string]any
	if err := json.Unmarshal(data, &actual); err != nil {
		return fmt.Errorf("decode LambdaDB index configs: %w", err)
	}
	if !reflect.DeepEqual(sortedKeys(actual), sortedKeys(want)) {
		return fmt.Errorf("LambdaDB index fields = %s, want %v", data, sortedKeys(want))
	}
	for field, wantConfig := range want {
		for key, value := range wantConfig {
			if !reflect.DeepEqual(actual[field][key], value) {
				return fmt.Errorf("LambdaDB index %q %s = %#v, want %#v (config %v)", field, key, actual[field][key], value, actual[field])
			}
		}
	}
	return nil
}

// compareLambdaDBDocs fetches every expected document with a consistent read and
// requires exactly the expected fields and values.
func compareLambdaDBDocs(ctx context.Context, client *sdk.Client, collection string, want map[string]map[string]any) error {
	ids := sortedKeys(want)
	result, err := client.Collection(collection).Docs().Fetch(ctx, sdk.FetchDocsInput{
		Ids:            ids,
		ConsistentRead: sdk.Bool(true),
		IncludeVectors: sdk.Bool(true),
	})
	if err != nil {
		return fmt.Errorf("fetch LambdaDB docs: %w", err)
	}
	if len(result.Docs) != len(ids) {
		return fmt.Errorf("fetched %d LambdaDB docs, want %d", len(result.Docs), len(ids))
	}
	for _, fetched := range result.Docs {
		id, _ := fetched.Doc["id"].(string)
		wantDoc, ok := want[id]
		if !ok {
			return fmt.Errorf("fetched unexpected LambdaDB doc %#v", fetched.Doc)
		}
		if !reflect.DeepEqual(sortedKeys(fetched.Doc), sortedKeys(wantDoc)) || !jsonValuesEqual(wantDoc, fetched.Doc) {
			return fmt.Errorf("LambdaDB doc %q = %#v, want %#v", id, fetched.Doc, wantDoc)
		}
	}
	return nil
}

// numDocs reflects the committed head, so it can trail consistent document reads.
func waitForLambdaDBNumDocs(ctx context.Context, client *sdk.Client, collection string, want int64) error {
	var last int64 = -1
	for {
		got, err := client.Collection(collection).Get(ctx)
		if err == nil {
			last = got.NumDocs
			if last == want {
				return nil
			}
			if last > want {
				return fmt.Errorf("LambdaDB collection %q numDocs = %d, want %d", collection, last, want)
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("LambdaDB collection %q numDocs = %d, want %d (last error %v): %w", collection, last, want, err, ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
}

func waitForLambdaDBCollectionAbsent(ctx context.Context, client *sdk.Client, collection string) error {
	for {
		_, err := client.Collection(collection).Get(ctx)
		var notFound *apierrors.ResourceNotFoundError
		if errors.As(err, &notFound) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("LambdaDB collection %q still exists after delete (last error %v): %w", collection, err, ctx.Err())
		case <-time.After(time.Second):
		}
	}
}

const cohereFixtureDims = 256

// cohereNativeSourcesMapping adds a Cohere native embedding field generated
// from two ordered text sources, one of them a nested stored path.
func cohereNativeSourcesMapping(targetCollection string) config.MappingConfig {
	mapping := additionalPayloadIndexTypesMapping(targetCollection)
	mapping.Payload.IndexConfigs["generated"] = map[string]any{
		"type": "vector",
		"embedding": map[string]any{
			"provider":   "cohere",
			"model":      "embed-v5.0-fast",
			"dimensions": cohereFixtureDims,
			"sources": []map[string]any{
				{"field": "body", "type": "text"},
				{"field": "attributes.section", "type": "text"},
			},
		},
	}
	return mapping
}

// generatedVector expects a server-generated vector of the given dimensions,
// whose values depend on the embedding model.
type generatedVector int

// jsonValuesEqual compares an expected value with a decoded JSON value,
// allowing float32 rounding for numbers.
func jsonValuesEqual(want, got any) bool {
	switch w := want.(type) {
	case generatedVector:
		g, ok := got.([]any)
		if !ok || len(g) != int(w) {
			return false
		}
		nonzero := false
		for _, value := range g {
			number, ok := value.(float64)
			if !ok {
				return false
			}
			nonzero = nonzero || number != 0
		}
		return nonzero
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok || len(g) != len(w) {
			return false
		}
		for key, value := range w {
			if !jsonValuesEqual(value, g[key]) {
				return false
			}
		}
		return true
	case []any:
		g, ok := got.([]any)
		if !ok || len(g) != len(w) {
			return false
		}
		for i := range w {
			if !jsonValuesEqual(w[i], g[i]) {
				return false
			}
		}
		return true
	case float64:
		g, ok := got.(float64)
		return ok && math.Abs(g-w) <= 1e-6
	default:
		return reflect.DeepEqual(want, got)
	}
}

// padded returns the fixtureVector values as decoded JSON.
func padded(values ...float64) []any {
	out := make([]any, fixtureVectorDims)
	for i := range out {
		out[i] = 0.0
	}
	for i, value := range values {
		out[i] = value
	}
	return out
}

// unit returns the normalized fixtureVector Qdrant stores for cosine collections.
func unit(values ...float64) []any {
	var sum float64
	for _, value := range values {
		sum += value * value
	}
	out := padded(values...)
	for i, value := range values {
		out[i] = value / math.Sqrt(sum)
	}
	return out
}

func largeDenseDocs(count int) map[string]map[string]any {
	docs := make(map[string]map[string]any, count)
	for i := 1; i <= count; i++ {
		id := strconv.Itoa(1000 + i)
		docs[id] = map[string]any{
			"id":    id,
			"title": fmt.Sprintf("large fixture %d", i),
			"rank":  float64(i),
			"dense": unit(float64(i%10)/10, float64((i+1)%10)/10, float64((i+2)%10)/10),
		}
	}
	return docs
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
