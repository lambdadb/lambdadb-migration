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
	ConnectionID   string `json:"connectionId"`
	DisconnectedAt *int64 `json:"disconnectedAt"`
	CollectionName string `json:"collectionName"`
	ProjectName    string `json:"projectName"`
}

func (t *APITarget) collectionPath() string {
	return "/projects/" + url.PathEscape(t.binding.Target.Project) + "/collections/" + url.PathEscape(t.binding.Target.Collection)
}
func (t *APITarget) request(ctx context.Context, method, path string, body []byte) ([]byte, int, error) {
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
// uncertain outcome is recovered by Describe, never by another creation request.
func (t *APITarget) CreateOnce(ctx context.Context) error {
	body, _ := json.Marshal(struct {
		Collection string          `json:"collectionName"`
		Indexes    json.RawMessage `json:"indexConfigs"`
		Connection string          `json:"connectionId"`
	}{t.binding.Target.Collection, t.binding.IndexConfigs, t.binding.ConnectionID})
	_, status, err := t.request(ctx, "POST", "/projects/"+url.PathEscape(t.binding.Target.Project)+"/collections", body)
	if err != nil {
		return err
	}
	if status != 200 && status != 201 && status != 202 {
		return fmt.Errorf("target creation unconfirmed (HTTP %d)", status)
	}
	return nil
}
func (t *APITarget) Disconnect(ctx context.Context) error {
	body, _ := json.Marshal(map[string]string{"connectionId": t.binding.ConnectionID})
	raw, status, err := t.request(ctx, "POST", t.collectionPath()+"/connection/disconnect", body)
	if err != nil {
		return err
	}
	if status != 202 {
		return fmt.Errorf("target disconnect unconfirmed (HTTP %d)", status)
	}
	var receipt struct {
		Connection string `json:"connectionId"`
		State      string `json:"state"`
	}
	if json.Unmarshal(raw, &receipt) != nil || receipt.Connection != t.binding.ConnectionID || receipt.State != "disconnected" {
		return errors.New("target disconnect receipt mismatch")
	}
	return nil
}
