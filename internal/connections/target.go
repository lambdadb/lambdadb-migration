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

// IntentTarget uses ordinary supported document APIs, without automatic retries.
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
	raw := intent.Content
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
		return ErrTargetOutcomeUncertain
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		if response.StatusCode >= 300 && response.StatusCode < 500 && response.StatusCode != 408 {
			return fmt.Errorf("%w (HTTP %d)", ErrTargetRejected, response.StatusCode)
		}
		return ErrTargetOutcomeUncertain
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 4097))
	var receipt struct {
		Message string `json:"message"`
	}
	if err != nil || len(data) > 4096 || json.Unmarshal(data, &receipt) != nil || receipt.Message == "" {
		return ErrTargetOutcomeUncertain
	}
	return nil
}

// RecoverPending sends only a prepared intent that has never started transmission.
func (j *Journal) RecoverPending(ctx context.Context, target IntentTarget) error {
	if err := j.PendingError(); err != nil {
		return err
	}
	s, err := j.Load()
	if err != nil {
		return err
	}
	if s.Pending == nil {
		return nil
	}
	return j.deliver(ctx, target, *s.Pending)
}
func (j *Journal) deliver(ctx context.Context, target IntentTarget, intent Intent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := j.markSending(intent); err != nil {
		return err
	}
	if err := target.Send(ctx, intent); err != nil {
		if errors.Is(err, ErrTargetRejected) {
			if saveErr := j.reject(intent); saveErr != nil {
				return ErrTargetOutcomeUncertain
			}
			return ErrTargetRejected
		}
		return ErrTargetOutcomeUncertain
	}
	if err := j.Accept(intent); err != nil {
		return ErrTargetOutcomeUncertain
	}
	return nil
}
func (j *Journal) Apply(ctx context.Context, target IntentTarget, operation string, content json.RawMessage) error {
	intent, err := j.Prepare(operation, content)
	if err != nil {
		return err
	}
	return j.deliver(ctx, target, *intent)
}
