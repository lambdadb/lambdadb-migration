package qdrant

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"

	qdrantapi "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"

	"github.com/lambdadb/lambdadb-migration/internal/config"
	"github.com/lambdadb/lambdadb-migration/internal/source"
)

type Source struct {
	cfg    config.QdrantConfig
	client *qdrantapi.Client
}

type cursorValue struct {
	Num  string `json:"num,omitempty"`
	UUID string `json:"uuid,omitempty"`
}

func New(cfg config.QdrantConfig) (*Source, error) {
	host, port, useTLS, err := parseURL(cfg.URL)
	if err != nil {
		return nil, err
	}

	var opts []grpc.DialOption
	if cfg.MaxMessageSize > 0 {
		opts = append(opts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(cfg.MaxMessageSize)))
	}

	client, err := qdrantapi.NewClient(&qdrantapi.Config{
		Host:                   host,
		Port:                   port,
		APIKey:                 cfg.APIKey,
		UseTLS:                 useTLS,
		TLSConfig:              &tls.Config{},
		GrpcOptions:            opts,
		SkipCompatibilityCheck: true,
	})
	if err != nil {
		return nil, fmt.Errorf("create qdrant client: %w", err)
	}

	return &Source{cfg: cfg, client: client}, nil
}

func (s *Source) Name() string {
	return "qdrant"
}

func (s *Source) Close() error {
	s.client.Close()
	return nil
}

func (s *Source) Count(ctx context.Context) (uint64, error) {
	count, err := s.client.Count(ctx, &qdrantapi.CountPoints{
		CollectionName: s.cfg.Collection,
		Exact:          qdrantapi.PtrOf(true),
	})
	if err != nil {
		return 0, fmt.Errorf("count qdrant points: %w", err)
	}
	return count, nil
}

func (s *Source) SearchDense(ctx context.Context, vectorName string, vector []float32, limit int) ([]string, error) {
	if limit < 1 {
		return nil, fmt.Errorf("search limit must be greater than 0")
	}
	exact := true
	var vectorNamePtr *string
	if vectorName != "" {
		vectorNamePtr = &vectorName
	}
	resp, err := s.client.GetPointsClient().Search(ctx, &qdrantapi.SearchPoints{
		CollectionName: s.cfg.Collection,
		Vector:         vector,
		VectorName:     vectorNamePtr,
		Limit:          uint64(limit),
		WithPayload:    qdrantapi.NewWithPayload(false),
		WithVectors:    qdrantapi.NewWithVectors(false),
		Params:         &qdrantapi.SearchParams{Exact: &exact},
	})
	if err != nil {
		return nil, fmt.Errorf("search qdrant points: %w", err)
	}
	ids := make([]string, 0, len(resp.GetResult()))
	for _, point := range resp.GetResult() {
		ids = append(ids, pointIDToString(point.GetId()))
	}
	return ids, nil
}

func (s *Source) SearchSparse(ctx context.Context, vectorName string, vector map[string]float32, limit int) ([]string, error) {
	if limit < 1 {
		return nil, fmt.Errorf("search limit must be greater than 0")
	}
	indices, values, err := sparseMapToQdrantVector(vector)
	if err != nil {
		return nil, err
	}
	exact := true
	var vectorNamePtr *string
	if vectorName != "" {
		vectorNamePtr = &vectorName
	}
	resp, err := s.client.GetPointsClient().Search(ctx, &qdrantapi.SearchPoints{
		CollectionName: s.cfg.Collection,
		Vector:         values,
		VectorName:     vectorNamePtr,
		Limit:          uint64(limit),
		WithPayload:    qdrantapi.NewWithPayload(false),
		WithVectors:    qdrantapi.NewWithVectors(false),
		Params:         &qdrantapi.SearchParams{Exact: &exact},
		SparseIndices:  &qdrantapi.SparseIndices{Data: indices},
	})
	if err != nil {
		return nil, fmt.Errorf("search qdrant sparse points: %w", err)
	}
	ids := make([]string, 0, len(resp.GetResult()))
	for _, point := range resp.GetResult() {
		ids = append(ids, pointIDToString(point.GetId()))
	}
	return ids, nil
}

func sparseMapToQdrantVector(vector map[string]float32) ([]uint32, []float32, error) {
	if len(vector) == 0 {
		return nil, nil, fmt.Errorf("sparse search vector must not be empty")
	}
	keys := make([]uint64, 0, len(vector))
	for key := range vector {
		index, err := strconv.ParseUint(key, 10, 32)
		if err != nil {
			return nil, nil, fmt.Errorf("parse sparse vector index %q: %w", key, err)
		}
		keys = append(keys, index)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	indices := make([]uint32, 0, len(keys))
	values := make([]float32, 0, len(keys))
	for _, key := range keys {
		keyString := strconv.FormatUint(key, 10)
		indices = append(indices, uint32(key))
		values = append(values, vector[keyString])
	}
	return indices, values, nil
}

func (s *Source) Read(ctx context.Context, cursor source.Cursor, limit int) (source.Batch, error) {
	offset, err := cursorToPointID(cursor)
	if err != nil {
		return source.Batch{}, err
	}
	if limit < 1 {
		return source.Batch{}, fmt.Errorf("read limit must be greater than 0")
	}
	uLimit := uint32(limit)
	resp, err := s.client.GetPointsClient().Scroll(ctx, &qdrantapi.ScrollPoints{
		CollectionName: s.cfg.Collection,
		Offset:         offset,
		Limit:          &uLimit,
		WithPayload:    qdrantapi.NewWithPayload(true),
		WithVectors:    qdrantapi.NewWithVectors(true),
	})
	if err != nil {
		return source.Batch{}, fmt.Errorf("scroll qdrant points: %w", err)
	}

	records := make([]source.Record, 0, len(resp.GetResult()))
	for _, point := range resp.GetResult() {
		records = append(records, retrievedPointToRecord(point))
	}

	nextOffset := resp.GetNextPageOffset()
	if nextOffset == nil {
		return source.Batch{Records: records, Done: true}, nil
	}
	return source.Batch{
		Records:    records,
		NextCursor: &source.Cursor{Value: pointIDToCursor(nextOffset)},
	}, nil
}

func cursorToPointID(cursor source.Cursor) (*qdrantapi.PointId, error) {
	if cursor.Value == nil {
		return nil, nil
	}
	if id, ok := cursor.Value.(*qdrantapi.PointId); ok {
		return id, nil
	}
	if value, ok := cursor.Value.(cursorValue); ok {
		return cursorValueToPointID(value)
	}
	if value, ok := cursor.Value.(*cursorValue); ok {
		return cursorValueToPointID(*value)
	}
	values, ok := cursor.Value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("unsupported qdrant cursor type %T", cursor.Value)
	}
	if uuid, ok := values["uuid"].(string); ok && uuid != "" {
		return qdrantapi.NewIDUUID(uuid), nil
	}
	if num, ok := values["num"].(string); ok && num != "" {
		parsed, err := strconv.ParseUint(num, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse qdrant numeric cursor %q: %w", num, err)
		}
		return qdrantapi.NewIDNum(parsed), nil
	}
	if num, ok := values["num"].(json.Number); ok {
		parsed, err := strconv.ParseUint(num.String(), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse qdrant numeric cursor %q: %w", num.String(), err)
		}
		return qdrantapi.NewIDNum(parsed), nil
	}
	if num, ok := values["num"].(float64); ok {
		return qdrantapi.NewIDNum(uint64(num)), nil
	}
	if num, ok := values["num"].(uint64); ok {
		return qdrantapi.NewIDNum(num), nil
	}
	return nil, fmt.Errorf("invalid qdrant cursor value")
}

func cursorValueToPointID(value cursorValue) (*qdrantapi.PointId, error) {
	if value.UUID != "" {
		return qdrantapi.NewIDUUID(value.UUID), nil
	}
	if value.Num != "" {
		parsed, err := strconv.ParseUint(value.Num, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse qdrant numeric cursor %q: %w", value.Num, err)
		}
		return qdrantapi.NewIDNum(parsed), nil
	}
	return nil, fmt.Errorf("invalid qdrant cursor value")
}

func pointIDToCursor(id *qdrantapi.PointId) cursorValue {
	if id.GetUuid() != "" {
		return cursorValue{UUID: id.GetUuid()}
	}
	return cursorValue{Num: strconv.FormatUint(id.GetNum(), 10)}
}

func parseURL(raw string) (host string, port int, useTLS bool, err error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", 0, false, fmt.Errorf("parse qdrant url: %w", err)
	}
	if parsed.Hostname() == "" {
		return "", 0, false, fmt.Errorf("parse qdrant url: missing host")
	}
	port = 6334
	if parsed.Port() != "" {
		port, err = strconv.Atoi(parsed.Port())
		if err != nil {
			return "", 0, false, fmt.Errorf("parse qdrant port: %w", err)
		}
	} else if parsed.Scheme == "https" {
		port = 443
	}
	return parsed.Hostname(), port, parsed.Scheme == "https", nil
}
