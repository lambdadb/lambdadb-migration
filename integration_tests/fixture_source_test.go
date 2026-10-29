package integration_tests

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/lambdadb/lambdadb-migration/internal/config"
	"github.com/lambdadb/lambdadb-migration/internal/source"
	qdrantsource "github.com/lambdadb/lambdadb-migration/internal/source/qdrant"
	pineconeapi "github.com/pinecone-io/go-pinecone/v5/pinecone"
	qdrantapi "github.com/qdrant/go-client/qdrant"
)

// fixtureSource is an in-memory source.Source that returns the inventory and
// records a source adapter produces. The fixture live suite uses it to run the
// migration pipeline against a real LambdaDB target without a source database.
type fixtureSource struct {
	inventory source.Inventory
	records   []source.Record
}

func (s *fixtureSource) Name() string { return s.inventory.SourceKind }

func (s *fixtureSource) Inventory(context.Context) (*source.Inventory, error) {
	inv := s.inventory
	inv.RecordCount = uint64(len(s.records))
	return &inv, nil
}

func (s *fixtureSource) Count(context.Context) (uint64, error) {
	return uint64(len(s.records)), nil
}

func (s *fixtureSource) Read(_ context.Context, cursor source.Cursor, limit int) (source.Batch, error) {
	if limit < 1 {
		return source.Batch{}, fmt.Errorf("read limit must be greater than 0")
	}
	offset := 0
	if cursor.Value != nil {
		value, ok := cursor.Value.(int)
		if !ok || value < 0 || value > len(s.records) {
			return source.Batch{}, fmt.Errorf("unexpected fixture cursor %#v", cursor.Value)
		}
		offset = value
	}
	end := min(offset+limit, len(s.records))
	batch := source.Batch{Records: s.records[offset:end]}
	if end == len(s.records) {
		batch.Done = true
	} else {
		batch.NextCursor = &source.Cursor{Value: end}
	}
	return batch, nil
}

// SearchDense ranks every record by the source vector's similarity, as an exact
// source-side search for query overlap validation.
func (s *fixtureSource) SearchDense(_ context.Context, vectorName string, query []float32, limit int) ([]string, error) {
	field, ok := s.inventory.Vectors[vectorName]
	if !ok {
		return nil, fmt.Errorf("fixture source has no dense vector %q", vectorName)
	}
	return s.rank(limit, func(record source.Record) (float64, bool) {
		vector := record.Vectors[vectorName].Dense
		if vector == nil {
			return 0, false
		}
		var dot, queryNorm, vectorNorm, distance float64
		for i := range min(len(query), len(vector)) {
			q, v := float64(query[i]), float64(vector[i])
			dot += q * v
			queryNorm += q * q
			vectorNorm += v * v
			distance += (q - v) * (q - v)
		}
		switch field.Similarity {
		case "cosine":
			return dot / math.Sqrt(queryNorm*vectorNorm), true
		case "euclidean":
			return -distance, true
		default:
			return dot, true
		}
	}), nil
}

// SearchSparse ranks records that share at least one sparse index by dot product.
func (s *fixtureSource) SearchSparse(_ context.Context, vectorName string, query map[string]float32, limit int) ([]string, error) {
	return s.rank(limit, func(record source.Record) (float64, bool) {
		var score float64
		matched := false
		for index, value := range record.Vectors[vectorName].Sparse {
			if q, ok := query[index]; ok {
				score += float64(q) * float64(value)
				matched = true
			}
		}
		return score, matched
	}), nil
}

func (s *fixtureSource) rank(limit int, score func(source.Record) (float64, bool)) []string {
	type scored struct {
		id    string
		score float64
	}
	var hits []scored
	for _, record := range s.records {
		if value, ok := score(record); ok {
			hits = append(hits, scored{id: record.ID, score: value})
		}
	}
	sort.SliceStable(hits, func(i, j int) bool { return hits[i].score > hits[j].score })
	ids := make([]string, 0, min(limit, len(hits)))
	for _, hit := range hits[:min(limit, len(hits))] {
		ids = append(ids, hit.id)
	}
	return ids
}

func newInventory(kind, collection string) source.Inventory {
	return source.Inventory{
		SourceKind:     kind,
		CollectionName: collection,
		Vectors:        map[string]source.VectorField{},
		SparseVectors:  map[string]source.SparseVectorField{},
		PayloadIndexes: map[string]source.PayloadIndex{},
	}
}

// qdrantFixtureSource converts a Qdrant fixture into the inventory and records
// that internal/source/qdrant returns for the same collection.
// TestQdrantFixtureSourceMatchesAdapter compares both against a local Qdrant.
func qdrantFixtureSource(collection string, fixture qdrantFixture) *fixtureSource {
	inv := newInventory("qdrant", collection)
	addQdrantVector := func(name string, params *qdrantapi.VectorParams) {
		fieldName := name
		if fieldName == "" {
			fieldName = "dense"
		}
		similarity := map[qdrantapi.Distance]string{
			qdrantapi.Distance_Cosine:    "cosine",
			qdrantapi.Distance_Euclid:    "euclidean",
			qdrantapi.Distance_Dot:       "dot_product",
			qdrantapi.Distance_Manhattan: "unsupported:manhattan",
		}[params.GetDistance()]
		inv.Vectors[name] = source.VectorField{Name: fieldName, Dimensions: int64(params.GetSize()), Similarity: similarity}
	}
	if params := fixture.vectorsConfig.GetParams(); params != nil {
		addQdrantVector("", params)
	}
	for name, params := range fixture.vectorsConfig.GetParamsMap().GetMap() {
		addQdrantVector(name, params)
	}
	for name := range fixture.sparseVectorsConfig.GetMap() {
		inv.SparseVectors[name] = source.SparseVectorField{Name: name}
	}
	for _, index := range fixture.payloadIndexes {
		typ := map[qdrantapi.FieldType]string{
			qdrantapi.FieldType_FieldTypeKeyword:  "keyword",
			qdrantapi.FieldType_FieldTypeInteger:  "long",
			qdrantapi.FieldType_FieldTypeFloat:    "double",
			qdrantapi.FieldType_FieldTypeText:     "text",
			qdrantapi.FieldType_FieldTypeBool:     "boolean",
			qdrantapi.FieldType_FieldTypeDatetime: "datetime",
		}[index.typ]
		inv.PayloadIndexes[index.field] = source.PayloadIndex{Name: index.field, Type: typ}
	}

	records := make([]source.Record, 0, len(fixture.points))
	for _, point := range fixture.points {
		record := source.Record{
			ID:      strconv.FormatUint(point.GetId().GetNum(), 10),
			Payload: map[string]any{},
			Vectors: map[string]source.VectorValue{},
		}
		if uuid := point.GetId().GetUuid(); uuid != "" {
			record.ID = uuid
		}
		for key, value := range point.GetPayload() {
			record.Payload[key] = qdrantValue(value)
		}
		if vector := point.GetVectors().GetVector(); vector != nil {
			record.Vectors[""] = qdrantVector(vector, inv.Vectors[""])
		}
		for name, vector := range point.GetVectors().GetVectors().GetVectors() {
			record.Vectors[name] = qdrantVector(vector, inv.Vectors[name])
		}
		records = append(records, record)
	}
	return &fixtureSource{inventory: inv, records: records}
}

func qdrantValue(value *qdrantapi.Value) any {
	switch v := value.GetKind().(type) {
	case *qdrantapi.Value_DoubleValue:
		return v.DoubleValue
	case *qdrantapi.Value_IntegerValue:
		return v.IntegerValue
	case *qdrantapi.Value_StringValue:
		return v.StringValue
	case *qdrantapi.Value_BoolValue:
		return v.BoolValue
	case *qdrantapi.Value_StructValue:
		out := map[string]any{}
		for key, nested := range v.StructValue.GetFields() {
			out[key] = qdrantValue(nested)
		}
		return out
	case *qdrantapi.Value_ListValue:
		out := []any{}
		for _, item := range v.ListValue.GetValues() {
			out = append(out, qdrantValue(item))
		}
		return out
	default:
		return nil
	}
}

// Qdrant stores cosine vectors normalized, so the adapter reads unit vectors.
// The result can differ from Qdrant's in the last float32 bit.
func qdrantVector(vector *qdrantapi.Vector, field source.VectorField) source.VectorValue {
	if sparse := vector.GetSparse(); sparse != nil {
		return source.VectorValue{Sparse: sparseIndexMap(sparse.GetIndices(), sparse.GetValues())}
	}
	data := vector.GetDense().GetData()
	if field.Similarity != "cosine" {
		return source.VectorValue{Dense: data}
	}
	var sum float32
	for _, value := range data {
		sum += value * value
	}
	length := float32(math.Sqrt(float64(sum)))
	normalized := make([]float32, len(data))
	for i, value := range data {
		normalized[i] = value / length
	}
	return source.VectorValue{Dense: normalized}
}

func sparseIndexMap(indices []uint32, values []float32) map[string]float32 {
	out := make(map[string]float32, len(indices))
	for i, index := range indices {
		out[strconv.FormatUint(uint64(index), 10)] = values[i]
	}
	return out
}

// pineconeFixtureSource converts Pinecone vectors into the inventory and records
// that internal/source/pinecone returns. A dense index exposes one unnamed
// vector; a sparse index exposes the "sparse" vector. Metadata indexes are not
// introspected, and metadata numbers decode as float64.
func pineconeFixtureSource(t *testing.T, index string, vectors []*pineconeapi.Vector, dense *source.VectorField) *fixtureSource {
	t.Helper()
	inv := newInventory("pinecone", index)
	if dense != nil {
		inv.Vectors[""] = *dense
	} else {
		inv.SparseVectors["sparse"] = source.SparseVectorField{Name: "sparse"}
	}
	records := make([]source.Record, 0, len(vectors))
	for _, vector := range vectors {
		record := source.Record{ID: vector.Id, Payload: vector.Metadata.AsMap(), Vectors: map[string]source.VectorValue{}}
		if vector.Values != nil {
			record.Vectors[""] = source.VectorValue{Dense: *vector.Values}
		}
		if vector.SparseValues != nil {
			record.Vectors["sparse"] = source.VectorValue{Sparse: sparseIndexMap(vector.SparseValues.Indices, vector.SparseValues.Values)}
		}
		records = append(records, record)
	}
	return &fixtureSource{inventory: inv, records: records}
}

// elasticsearchTextAnalyzerFixture mirrors what internal/source/elasticsearch
// returns for this index:
//
//	settings: no index-level default analyzer
//	mappings: title text (analyzer english), body text (analyzer standard),
//	          notes text (no analyzer), tags keyword,
//	          stats object {views long, rating scaled_float}, published_at date,
//	          is_public boolean, embedding dense_vector (dims 64, l2_norm)
//
// Nested object fields are flattened into dotted payload paths, numbers decode
// as json.Number, and dense_vector values are returned as named vectors.
func elasticsearchTextAnalyzerFixture(index string) *fixtureSource {
	inv := newInventory("elasticsearch", index)
	inv.Vectors["embedding"] = source.VectorField{Name: "embedding", Dimensions: fixtureVectorDims, Similarity: "euclidean"}
	for _, field := range []source.PayloadIndex{
		{Name: "title", Type: "text", Analyzers: []string{"english"}},
		{Name: "body", Type: "text", Analyzers: []string{"standard"}},
		{Name: "notes", Type: "text"},
		{Name: "tags", Type: "keyword"},
		{Name: "stats.views", Type: "long"},
		{Name: "stats.rating", Type: "double"},
		{Name: "published_at", Type: "datetime"},
		{Name: "is_public", Type: "boolean"},
	} {
		inv.PayloadIndexes[field.Name] = field
	}
	record := func(id, title, body, notes, tags, views, rating, publishedAt string, public bool, embedding []float32) source.Record {
		return source.Record{
			ID: id,
			Payload: map[string]any{
				"title":        title,
				"body":         body,
				"notes":        notes,
				"tags":         tags,
				"stats.views":  json.Number(views),
				"stats.rating": json.Number(rating),
				"published_at": publishedAt,
				"is_public":    public,
			},
			Vectors: map[string]source.VectorValue{"embedding": {Dense: embedding}},
		}
	}
	return &fixtureSource{inventory: inv, records: []source.Record{
		record("es-1", "Migrating search indexes", "Analyzers map to LambdaDB presets", "server default analyzer", "search", "12", "4.5", "2026-05-10T00:00:00Z", true, fixtureVector(0.1, 0.2, 0.3)),
		record("es-2", "Running queries", "Standard tokenization keeps words", "second note", "query", "34", "3.25", "2026-05-11T00:00:00Z", false, fixtureVector(0.4, 0.5, 0.6)),
	}}
}

// The fixture live suite replaces Qdrant with qdrantFixtureSource. Keep that
// conversion equal to the real adapter's output for every Qdrant fixture.
func TestQdrantFixtureSourceMatchesAdapter(t *testing.T) {
	if !envEnabled("LAMBDADB_MIGRATION_RUN_QDRANT_MOCK_E2E", "LAMBDADB_MIGRATION_RUN_INTEGRATION") {
		t.Skip("set LAMBDADB_MIGRATION_RUN_QDRANT_MOCK_E2E=1 and run Qdrant from integration_tests/compose/qdrant.yaml")
	}
	qdrantURL := os.Getenv("LAMBDADB_MIGRATION_QDRANT_URL")
	if qdrantURL == "" {
		qdrantURL = "http://localhost:6334"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for name, fixture := range map[string]qdrantFixture{
		"unnamed_dense":         unnamedDenseFixture(),
		"named_dense":           namedDenseFixture(),
		"dense_sparse_payload":  denseSparsePayloadIndexFixture(),
		"additional_index_type": additionalPayloadIndexTypesFixture(),
		"large_dense":           largeDenseFixture(5),
	} {
		t.Run(name, func(t *testing.T) {
			collection := fmt.Sprintf("lambdadb_migration_fixture_%s_%d", name, time.Now().UnixNano())
			seedQdrantCollection(t, ctx, qdrantURL, collection, fixture)
			adapter, err := qdrantsource.New(config.QdrantConfig{URL: qdrantURL, Collection: collection, MaxMessageSize: 32 * 1024 * 1024})
			if err != nil {
				t.Fatal(err)
			}
			defer adapter.Close()

			fixtureSrc := qdrantFixtureSource(collection, fixture)
			got, err := adapter.Inventory(ctx)
			if err != nil {
				t.Fatal(err)
			}
			got.Warnings = nil // Warnings are printed, not migrated.
			want, _ := fixtureSrc.Inventory(ctx)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("adapter inventory = %#v\nfixture inventory = %#v", got, want)
			}
			gotRecords, wantRecords := readAllRecords(t, ctx, adapter), readAllRecords(t, ctx, fixtureSrc)
			if !recordsMatch(gotRecords, wantRecords) {
				t.Fatalf("adapter records = %#v\nfixture records = %#v", gotRecords, wantRecords)
			}
		})
	}
}

func readAllRecords(t *testing.T, ctx context.Context, src source.Source) []source.Record {
	t.Helper()
	var records []source.Record
	cursor := source.Cursor{}
	for {
		batch, err := src.Read(ctx, cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, batch.Records...)
		if batch.Done {
			break
		}
		cursor = *batch.NextCursor
	}
	sort.Slice(records, func(i, j int) bool { return records[i].ID < records[j].ID })
	return records
}

// recordsMatch compares records exactly, except dense values within float32 rounding.
func recordsMatch(got, want []source.Record) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i].ID != want[i].ID || !reflect.DeepEqual(got[i].Payload, want[i].Payload) || len(got[i].Vectors) != len(want[i].Vectors) {
			return false
		}
		for name, wantVector := range want[i].Vectors {
			gotVector, ok := got[i].Vectors[name]
			if !ok || !reflect.DeepEqual(gotVector.Sparse, wantVector.Sparse) || len(gotVector.Dense) != len(wantVector.Dense) {
				return false
			}
			for j := range wantVector.Dense {
				if math.Abs(float64(gotVector.Dense[j]-wantVector.Dense[j])) > 1e-6 {
					return false
				}
			}
		}
	}
	return true
}
