package qdrant

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
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
	values, ok := cursor.Value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("unsupported qdrant cursor type %T", cursor.Value)
	}
	if uuid, ok := values["uuid"].(string); ok && uuid != "" {
		return qdrantapi.NewIDUUID(uuid), nil
	}
	if num, ok := values["num"].(float64); ok {
		return qdrantapi.NewIDNum(uint64(num)), nil
	}
	if num, ok := values["num"].(uint64); ok {
		return qdrantapi.NewIDNum(num), nil
	}
	return nil, fmt.Errorf("invalid qdrant cursor value")
}

func pointIDToCursor(id *qdrantapi.PointId) map[string]any {
	if id.GetUuid() != "" {
		return map[string]any{"uuid": id.GetUuid()}
	}
	return map[string]any{"num": id.GetNum()}
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
