package pinecone

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	pineconeapi "github.com/pinecone-io/go-pinecone/v5/pinecone"

	"github.com/lambdadb/lambdadb-migration/internal/config"
	"github.com/lambdadb/lambdadb-migration/internal/source"
)

type Source struct {
	cfg    config.PineconeConfig
	client *pineconeapi.Client
	index  *pineconeapi.Index
	conn   *pineconeapi.IndexConnection
}

func New(cfg config.PineconeConfig) (*Source, error) {
	client, err := pineconeapi.NewClient(pineconeapi.NewClientParams{
		ApiKey:    cfg.APIKey,
		Host:      cfg.Host,
		SourceTag: "lambdadb-migration",
	})
	if err != nil {
		return nil, fmt.Errorf("create pinecone client: %w", err)
	}
	return &Source{cfg: cfg, client: client}, nil
}

func (s *Source) Name() string {
	return "pinecone"
}

func (s *Source) Close() error {
	if s.conn == nil {
		return nil
	}
	return s.conn.Close()
}

func (s *Source) Count(ctx context.Context) (uint64, error) {
	if err := s.ensureIndex(ctx); err != nil {
		return 0, err
	}
	stats, err := s.conn.DescribeIndexStats(ctx)
	if err != nil {
		return 0, fmt.Errorf("describe pinecone index stats: %w", err)
	}
	if summary, ok := stats.Namespaces[s.cfg.Namespace]; ok && summary != nil {
		return uint64(summary.VectorCount), nil
	}
	if s.cfg.Namespace == "" {
		return uint64(stats.TotalVectorCount), nil
	}
	return 0, nil
}

func (s *Source) Read(ctx context.Context, cursor source.Cursor, limit int) (source.Batch, error) {
	if err := s.ensureIndex(ctx); err != nil {
		return source.Batch{}, err
	}
	if limit < 1 {
		return source.Batch{}, fmt.Errorf("read limit must be greater than 0")
	}
	token, err := cursorToToken(cursor)
	if err != nil {
		return source.Batch{}, err
	}
	uLimit := uint32(limit)
	req := &pineconeapi.ListVectorsRequest{
		Limit:           &uLimit,
		PaginationToken: token,
	}
	if s.cfg.ListPrefix != "" {
		req.Prefix = &s.cfg.ListPrefix
	}
	list, err := s.conn.ListVectors(ctx, req)
	if err != nil {
		return source.Batch{}, fmt.Errorf("list pinecone vectors: %w", err)
	}

	ids := vectorIDs(list.VectorIds)
	if len(ids) == 0 {
		if list.NextPaginationToken == nil || *list.NextPaginationToken == "" {
			return source.Batch{Done: true}, nil
		}
		return source.Batch{NextCursor: &source.Cursor{Value: *list.NextPaginationToken}}, nil
	}
	fetched, err := s.conn.FetchVectors(ctx, ids)
	if err != nil {
		return source.Batch{}, fmt.Errorf("fetch pinecone vectors: %w", err)
	}

	records := make([]source.Record, 0, len(ids))
	for _, id := range ids {
		vector := fetched.Vectors[id]
		if vector == nil {
			return source.Batch{}, fmt.Errorf("pinecone listed vector %q but fetch did not return it", id)
		}
		records = append(records, vectorToRecord(vector))
	}
	if list.NextPaginationToken == nil || *list.NextPaginationToken == "" {
		return source.Batch{Records: records, Done: true}, nil
	}
	return source.Batch{
		Records:    records,
		NextCursor: &source.Cursor{Value: *list.NextPaginationToken},
	}, nil
}

func (s *Source) SearchDense(ctx context.Context, vectorName string, vector []float32, limit int) ([]string, error) {
	if vectorName != "" && vectorName != "dense" {
		return nil, fmt.Errorf("pinecone source has a single dense vector field, got %q", vectorName)
	}
	if limit < 1 {
		return nil, fmt.Errorf("search limit must be greater than 0")
	}
	if err := s.ensureIndex(ctx); err != nil {
		return nil, err
	}
	resp, err := s.conn.QueryByVectorValues(ctx, &pineconeapi.QueryByVectorValuesRequest{
		Vector:          vector,
		TopK:            uint32(limit),
		IncludeValues:   false,
		IncludeMetadata: false,
	})
	if err != nil {
		return nil, fmt.Errorf("query pinecone vectors: %w", err)
	}
	ids := make([]string, 0, len(resp.Matches))
	for _, match := range resp.Matches {
		if match == nil || match.Vector == nil {
			continue
		}
		ids = append(ids, match.Vector.Id)
	}
	return ids, nil
}

func (s *Source) ensureIndex(ctx context.Context) error {
	if s.conn != nil {
		return nil
	}
	index, err := s.client.DescribeIndex(ctx, s.cfg.Index)
	if err != nil {
		return fmt.Errorf("describe pinecone index: %w", err)
	}
	if index.Host == "" {
		return fmt.Errorf("pinecone index %q has no data-plane host", s.cfg.Index)
	}
	conn, err := s.client.Index(pineconeapi.NewIndexConnParams{
		Host:      index.Host,
		Namespace: s.cfg.Namespace,
	})
	if err != nil {
		return fmt.Errorf("connect pinecone index: %w", err)
	}
	s.index = index
	s.conn = conn
	return nil
}

func cursorToToken(cursor source.Cursor) (*string, error) {
	if cursor.Value == nil {
		return nil, nil
	}
	switch value := cursor.Value.(type) {
	case string:
		if value == "" {
			return nil, nil
		}
		return &value, nil
	case json.Number:
		token := value.String()
		return &token, nil
	default:
		return nil, fmt.Errorf("unsupported pinecone cursor type %T", cursor.Value)
	}
}

func vectorIDs(values []*string) []string {
	ids := make([]string, 0, len(values))
	for _, value := range values {
		if value == nil || *value == "" {
			continue
		}
		ids = append(ids, *value)
	}
	return ids
}

func vectorToRecord(vector *pineconeapi.Vector) source.Record {
	return source.Record{
		ID:      vector.Id,
		Payload: metadataToMap(vector.Metadata),
		Vectors: vectorValuesToMap(vector),
	}
}

func metadataToMap(metadata *pineconeapi.Metadata) map[string]any {
	if metadata == nil {
		return map[string]any{}
	}
	return metadata.AsMap()
}

func vectorValuesToMap(vector *pineconeapi.Vector) map[string]source.VectorValue {
	out := map[string]source.VectorValue{}
	if vector.Values != nil {
		values := append([]float32(nil), (*vector.Values)...)
		out[""] = source.VectorValue{Dense: values}
	}
	if vector.SparseValues != nil {
		out["sparse"] = source.VectorValue{Sparse: sparseToMap(vector.SparseValues)}
	}
	return out
}

func sparseToMap(sparse *pineconeapi.SparseValues) map[string]float32 {
	out := make(map[string]float32, len(sparse.Indices))
	for i, index := range sparse.Indices {
		if i >= len(sparse.Values) {
			break
		}
		out[strconv.FormatUint(uint64(index), 10)] = sparse.Values[i]
	}
	return out
}
