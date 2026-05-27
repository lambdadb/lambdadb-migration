package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/lambdadb/lambdadb-migration/internal/config"
	"github.com/lambdadb/lambdadb-migration/internal/source"
)

type Source struct {
	cfg    config.ElasticsearchConfig
	client *http.Client
	base   *url.URL

	inventory *source.Inventory
}

type cursorValue struct {
	PITID       string `json:"pitId" yaml:"pitId"`
	SearchAfter []any  `json:"searchAfter,omitempty" yaml:"searchAfter,omitempty"`
}

func New(cfg config.ElasticsearchConfig) (*Source, error) {
	if cfg.PITKeepAlive == "" {
		cfg.PITKeepAlive = "5m"
	}
	base, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse elasticsearch url: %w", err)
	}
	if base.Scheme == "" || base.Host == "" {
		return nil, fmt.Errorf("parse elasticsearch url: missing scheme or host")
	}
	return &Source{
		cfg:    cfg,
		client: http.DefaultClient,
		base:   base,
	}, nil
}

func (s *Source) Name() string {
	return "elasticsearch"
}

func (s *Source) Close() error {
	return nil
}

func (s *Source) Count(ctx context.Context) (uint64, error) {
	var resp countResponse
	if err := s.do(ctx, http.MethodGet, "/"+pathEscape(s.cfg.Index)+"/_count", nil, &resp); err != nil {
		return 0, fmt.Errorf("count elasticsearch documents: %w", err)
	}
	if resp.Count < 0 {
		return 0, fmt.Errorf("elasticsearch returned negative count %d", resp.Count)
	}
	return uint64(resp.Count), nil
}

func (s *Source) Read(ctx context.Context, cursor source.Cursor, limit int) (source.Batch, error) {
	if limit < 1 {
		return source.Batch{}, fmt.Errorf("read limit must be greater than 0")
	}
	cur, err := parseCursor(cursor)
	if err != nil {
		return source.Batch{}, err
	}
	if cur.PITID == "" {
		pitID, err := s.openPIT(ctx)
		if err != nil {
			return source.Batch{}, err
		}
		cur.PITID = pitID
	}

	vectorFields, err := s.vectorFields(ctx)
	if err != nil {
		return source.Batch{}, err
	}
	req := searchRequest{
		Size:        limit,
		Query:       map[string]any{"match_all": map[string]any{}},
		PIT:         pitRequest{ID: cur.PITID, KeepAlive: s.cfg.PITKeepAlive},
		Sort:        []map[string]string{{"_shard_doc": "asc"}},
		SearchAfter: cur.SearchAfter,
		Fields:      vectorFields,
	}
	var resp searchResponse
	if err := s.do(ctx, http.MethodPost, "/_search", req, &resp); err != nil {
		return source.Batch{}, fmt.Errorf("search elasticsearch documents: %w", err)
	}
	pitID := resp.PITID
	if pitID == "" {
		pitID = cur.PITID
	}
	if len(resp.Hits.Hits) == 0 {
		_ = s.closePIT(ctx, pitID)
		return source.Batch{Done: true}, nil
	}

	vectorSet := stringSet(vectorFields)
	records := make([]source.Record, 0, len(resp.Hits.Hits))
	for _, hit := range resp.Hits.Hits {
		record, err := hitToRecord(hit, vectorSet)
		if err != nil {
			return source.Batch{}, err
		}
		records = append(records, record)
	}

	next := cursorValue{
		PITID:       pitID,
		SearchAfter: cloneAnySlice(resp.Hits.Hits[len(resp.Hits.Hits)-1].Sort),
	}
	return source.Batch{
		Records:    records,
		NextCursor: &source.Cursor{Value: next},
	}, nil
}

func (s *Source) openPIT(ctx context.Context) (string, error) {
	var resp pitResponse
	path := "/" + pathEscape(s.cfg.Index) + "/_pit?keep_alive=" + url.QueryEscape(s.cfg.PITKeepAlive)
	if err := s.do(ctx, http.MethodPost, path, nil, &resp); err != nil {
		return "", fmt.Errorf("open elasticsearch point in time: %w", err)
	}
	if resp.ID == "" {
		return "", fmt.Errorf("open elasticsearch point in time: response did not include id")
	}
	return resp.ID, nil
}

func (s *Source) closePIT(ctx context.Context, pitID string) error {
	if pitID == "" {
		return nil
	}
	req := map[string]any{"id": pitID}
	var resp map[string]any
	if err := s.do(ctx, http.MethodDelete, "/_pit", req, &resp); err != nil {
		return fmt.Errorf("close elasticsearch point in time: %w", err)
	}
	return nil
}

func (s *Source) vectorFields(ctx context.Context) ([]string, error) {
	if len(s.cfg.VectorFields) > 0 {
		fields := append([]string(nil), s.cfg.VectorFields...)
		sort.Strings(fields)
		return fields, nil
	}
	inv, err := s.Inventory(ctx)
	if err != nil {
		return nil, err
	}
	fields := make([]string, 0, len(inv.Vectors))
	for name := range inv.Vectors {
		fields = append(fields, name)
	}
	sort.Strings(fields)
	return fields, nil
}

func (s *Source) do(ctx context.Context, method, path string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode request body: %w", err)
		}
		reader = bytes.NewReader(data)
	}
	endpoint := s.base.ResolveReference(&url.URL{Path: strings.TrimRight(s.base.Path, "/") + path})
	if strings.Contains(path, "?") {
		parts := strings.SplitN(path, "?", 2)
		endpoint = s.base.ResolveReference(&url.URL{Path: strings.TrimRight(s.base.Path, "/") + parts[0], RawQuery: parts[1]})
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint.String(), reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if s.cfg.APIKey != "" {
		req.Header.Set("Authorization", "ApiKey "+s.cfg.APIKey)
	} else if s.cfg.Username != "" || s.cfg.Password != "" {
		req.SetBasicAuth(s.cfg.Username, s.cfg.Password)
	}

	res, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		return fmt.Errorf("elasticsearch %s %s returned HTTP %d: %s", method, path, res.StatusCode, strings.TrimSpace(string(data)))
	}
	if out == nil {
		return nil
	}
	dec := json.NewDecoder(res.Body)
	dec.UseNumber()
	if err := dec.Decode(out); err != nil {
		return fmt.Errorf("decode response body: %w", err)
	}
	return nil
}

func parseCursor(cursor source.Cursor) (cursorValue, error) {
	if cursor.Value == nil {
		return cursorValue{}, nil
	}
	if value, ok := cursor.Value.(cursorValue); ok {
		return value, nil
	}
	if value, ok := cursor.Value.(*cursorValue); ok && value != nil {
		return *value, nil
	}
	values, ok := cursor.Value.(map[string]any)
	if !ok {
		return cursorValue{}, fmt.Errorf("unsupported elasticsearch cursor type %T", cursor.Value)
	}
	var out cursorValue
	if pitID, ok := values["pitId"].(string); ok {
		out.PITID = pitID
	}
	if raw, ok := values["searchAfter"].([]any); ok {
		out.SearchAfter = cloneAnySlice(raw)
	}
	return out, nil
}

func pathEscape(value string) string {
	return url.PathEscape(value)
}

func stringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		out[value] = struct{}{}
	}
	return out
}

func cloneAnySlice(values []any) []any {
	if len(values) == 0 {
		return nil
	}
	out := make([]any, len(values))
	copy(out, values)
	return out
}

type countResponse struct {
	Count int64 `json:"count"`
}

type pitResponse struct {
	ID string `json:"id"`
}

type searchRequest struct {
	Size        int                 `json:"size"`
	Query       map[string]any      `json:"query"`
	PIT         pitRequest          `json:"pit"`
	Sort        []map[string]string `json:"sort"`
	SearchAfter []any               `json:"search_after,omitempty"`
	Fields      []string            `json:"fields,omitempty"`
}

type pitRequest struct {
	ID        string `json:"id"`
	KeepAlive string `json:"keep_alive"`
}

type searchResponse struct {
	PITID string `json:"pit_id"`
	Hits  struct {
		Hits []hit `json:"hits"`
	} `json:"hits"`
}

type hit struct {
	ID     string         `json:"_id"`
	Source rawObject      `json:"_source"`
	Fields map[string]any `json:"fields"`
	Sort   []any          `json:"sort"`
}
