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
	"time"

	"github.com/lambdadb/lambdadb-migration/internal/connections/protocol"
)

// IntentTarget acknowledges only the target's durable ordered acceptance.
// A generic 202 response does not satisfy this contract.
type IntentTarget interface {
	Send(context.Context, Intent) error
}

type APITarget struct {
	binding protocol.Binding
	key     string
	client  *http.Client
}

func NewAPITarget(binding protocol.Binding, key string) (*APITarget, error) {
	if !protocol.Origin(binding.Target.URL) || !protocol.CollectionName.MatchString(binding.Target.Collection) || binding.Target.Project == "" || !protocol.Identifier.MatchString(binding.ConnectionID) || binding.Target.Version < 3 || key == "" || (binding.Target.SharedGateway && binding.Target.ProjectID == "") {
		return nil, errors.New("invalid fixed target identity or credentials")
	}
	return &APITarget{binding: binding, key: key, client: &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (t *APITarget) Send(ctx context.Context, intent Intent) error {
	if !validIntent(intent) {
		return errors.New("invalid durable target intent")
	}
	var content map[string]json.RawMessage
	if json.Unmarshal(intent.Content, &content) != nil || len(content) != 1 {
		return errors.New("invalid target operation content")
	}
	field := "docs"
	if intent.Operation == "delete" {
		field = "ids"
	}
	if content[field] == nil {
		return errors.New("target content does not match operation")
	}
	order := struct {
		ConnectionID string `json:"connectionId"`
		Sequence     uint64 `json:"sequence"`
	}{t.binding.ConnectionID, intent.Sequence}
	content["ingestion"], _ = json.Marshal(order)
	raw, err := json.Marshal(content)
	if err != nil {
		return errors.New("cannot encode target intent")
	}
	path := strings.TrimRight(t.binding.Target.URL, "/") + "/projects/" + url.PathEscape(t.binding.Target.Project) + "/collections/" + url.PathEscape(t.binding.Target.Collection) + "/docs/" + intent.Operation
	req, err := http.NewRequestWithContext(ctx, "POST", path, bytes.NewReader(raw))
	if err != nil {
		return errors.New("invalid target request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("x-api-key", t.key)
	if t.binding.Target.SharedGateway {
		req.Header.Set("x-lambdadb-version", "v"+strconv.Itoa(t.binding.Target.Version))
		req.Header.Set("x-lambdadb-project-id", t.binding.Target.ProjectID)
	}
	response, err := t.client.Do(req)
	if err != nil {
		return errors.New("target outcome uncertain; retain pending intent")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		return fmt.Errorf("target ordered mutation not accepted (HTTP %d)", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 4097))
	if err != nil || len(data) > 4096 {
		return errors.New("invalid target acceptance")
	}
	var receipt struct {
		Ingestion *struct {
			ConnectionID string `json:"connectionId"`
			Sequence     uint64 `json:"sequence"`
		} `json:"ingestion"`
	}
	if json.Unmarshal(data, &receipt) != nil || receipt.Ingestion == nil || receipt.Ingestion.ConnectionID != order.ConnectionID || receipt.Ingestion.Sequence != order.Sequence {
		return errors.New("target lacks matching ordered acceptance; retain pending intent")
	}
	return nil
}
func (j *Journal) RecoverPending(ctx context.Context, target IntentTarget) error {
	s, err := j.Load()
	if err != nil {
		return err
	}
	if s.Pending == nil {
		return nil
	}
	if err = target.Send(ctx, *s.Pending); err != nil {
		return err
	}
	return j.Accept(*s.Pending)
}
func (j *Journal) Apply(ctx context.Context, target IntentTarget, operation string, content json.RawMessage) error {
	intent, err := j.Prepare(operation, content)
	if err != nil {
		return err
	}
	if err = target.Send(ctx, *intent); err != nil {
		return err
	}
	return j.Accept(*intent)
}
