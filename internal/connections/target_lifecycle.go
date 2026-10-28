package connections

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type CollectionMetadata struct {
	CollectionName string `json:"collectionName"`
	ProjectName    string `json:"projectName"`
}

func (t *APITarget) collectionPath() string {
	return "/projects/" + url.PathEscape(t.binding.Target.Project) + "/collections/" + url.PathEscape(t.binding.Target.Collection)
}
func (t *APITarget) request(ctx context.Context, method, path string, body []byte) ([]byte, int, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, unsentError{err}
	}
	// Drain admitted mutations on pause/shutdown, bounded by the client's timeout.
	if method == "POST" {
		ctx = context.WithoutCancel(ctx)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(t.binding.Target.URL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return nil, 0, errors.New("invalid target request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("x-api-key", t.key)
	if t.binding.Target.SharedGateway {
		req.Header.Set("x-lambdadb-version", "v"+strconv.Itoa(t.binding.Target.Version))
		req.Header.Set("x-lambdadb-project-id", t.binding.Target.ProjectID)
	}
	res, err := t.client.Do(req)
	if err != nil {
		return nil, 0, errors.New("target outcome uncertain")
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusTooManyRequests {
		return nil, res.StatusCode, rateLimited(res.Header.Get("Retry-After"))
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, (64<<10)+1))
	if err != nil || len(raw) > 64<<10 {
		return nil, res.StatusCode, errors.New("invalid target response")
	}
	return raw, res.StatusCode, nil
}
func (t *APITarget) Describe(ctx context.Context) (*CollectionMetadata, error) {
	raw, status, err := t.request(ctx, "GET", t.collectionPath(), nil)
	if err != nil {
		return nil, err
	}
	if status == 404 {
		return nil, nil
	}
	if status != 200 {
		return nil, fmt.Errorf("target metadata unavailable (HTTP %d)", status)
	}
	var response struct {
		Collection *CollectionMetadata `json:"collection"`
	}
	if json.Unmarshal(raw, &response) != nil || response.Collection == nil || response.Collection.CollectionName != t.binding.Target.Collection || response.Collection.ProjectName != t.binding.Target.Project {
		return nil, errors.New("invalid target collection metadata")
	}
	return response.Collection, nil
}

// CreateOnce must be preceded by a durable local creation-attempt marker. An
// uncertain outcome requires intervention; metadata alone cannot prove ownership.
func (t *APITarget) CreateOnce(ctx context.Context) error {
	body, _ := json.Marshal(struct {
		Collection string          `json:"collectionName"`
		Indexes    json.RawMessage `json:"indexConfigs"`
	}{t.binding.Target.Collection, t.binding.IndexConfigs})
	_, status, err := t.request(ctx, "POST", "/projects/"+url.PathEscape(t.binding.Target.Project)+"/collections", body)
	if err != nil {
		return err
	}
	if status != http.StatusCreated {
		if status >= 300 && status < 500 && status != 408 {
			return ErrTargetRejected
		}
		return ErrTargetOutcomeUncertain
	}
	return nil
}
