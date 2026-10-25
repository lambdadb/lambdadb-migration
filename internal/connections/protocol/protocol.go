// Package protocol defines metadata exchanged with the regional connection service.
// It contains secret references, never customer credentials or replicated rows.
package protocol

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const MaxBody = 64 << 10

var Identifier = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,80}$`)
var DatasetName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,39}$`)
var CollectionName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,59}$`)
var environmentName = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,127}$`)
var postgresOffset = regexp.MustCompile(`^[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}$`)

type Source struct {
	Type          string          `json:"type"`
	CredentialRef string          `json:"credential_ref"`
	Configuration json.RawMessage `json:"configuration"`
}
type Target struct {
	ProjectID     string `json:"project_id,omitempty"`
	Version       int    `json:"version"`
	SharedGateway bool   `json:"shared_gateway,omitempty"`
	URL           string `json:"url"`
	Project       string `json:"project"`
	Collection    string `json:"collection"`
	CredentialRef string `json:"credential_ref"`
}
type Binding struct {
	ConnectionID string          `json:"connection_id"`
	Revision     uint64          `json:"revision"`
	Region       string          `json:"region"`
	Mode         string          `json:"mode"`
	Source       Source          `json:"source"`
	Target       Target          `json:"target"`
	IndexConfigs json.RawMessage `json:"index_configs"`
}
type Command struct {
	ProtocolVersion    int       `json:"protocol_version"`
	ConnectionID       string    `json:"connection_id"`
	DesiredRevision    uint64    `json:"desired_revision"`
	DesiredState       string    `json:"desired_state"`
	AuthorityExpiresAt time.Time `json:"authority_expires_at"`
	ResyncRevision     uint64    `json:"resync_revision,omitempty"`
}
type Approved struct {
	Binding Binding `json:"binding"`
	Control Command `json:"control"`
}
type Position struct {
	SourceType string `json:"source_type"`
	Offset     string `json:"offset"`
}
type Observation struct {
	ProtocolVersion         int       `json:"protocol_version"`
	ConnectionID            string    `json:"connection_id"`
	DesiredRevision         uint64    `json:"desired_revision"`
	Sequence                uint64    `json:"sequence"`
	Phase                   string    `json:"phase"`
	ReasonCode              string    `json:"reason_code,omitempty"`
	Position                *Position `json:"position,omitempty"`
	Rows                    uint64    `json:"rows"`
	CompletedResyncRevision uint64    `json:"completed_resync_revision,omitempty"`
}
type Observed struct {
	Observation Observation `json:"observation"`
	ReceivedAt  time.Time   `json:"received_at"`
}
type PostgresSelection struct {
	Schema      string   `json:"schema"`
	Table       string   `json:"table"`
	Publication string   `json:"publication,omitempty"`
	Columns     []string `json:"columns,omitempty"`
}

func Decode(r io.Reader, v any) error {
	data, err := io.ReadAll(io.LimitReader(r, MaxBody+1))
	if err != nil || len(data) > MaxBody {
		return errors.New("invalid or oversized connection document")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(v) != nil {
		return errors.New("invalid connection document")
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return errors.New("trailing connection document")
	}
	return nil
}
func Origin(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.Host != "" && u.User == nil && u.RawQuery == "" && u.Fragment == "" && (u.Path == "" || u.Path == "/")
}
func CredentialReference(ref, region string) bool {
	if strings.HasPrefix(ref, "env:") {
		return environmentName.MatchString(strings.TrimPrefix(ref, "env:"))
	}
	parts := strings.SplitN(ref, ":", 7)
	return len(parts) == 7 && parts[0] == "arn" && (parts[1] == "aws" || parts[1] == "aws-us-gov" || parts[1] == "aws-cn") && parts[2] == "secretsmanager" && parts[3] == region && parts[4] != "" && parts[5] == "secret" && parts[6] != "" && len(ref) <= 2048
}
func ValidateSelection(p PostgresSelection, mode string) error {
	if !DatasetName.MatchString(p.Schema) || !DatasetName.MatchString(p.Table) || (mode == "continuous" && !DatasetName.MatchString(p.Publication)) || (p.Publication != "" && (!DatasetName.MatchString(p.Publication) || mode == "snapshot")) || len(p.Columns) > 256 {
		return errors.New("invalid PostgreSQL selection")
	}
	seen := map[string]bool{}
	for _, column := range p.Columns {
		if column == "" || len(column) > 63 || strings.ContainsRune(column, 0) || seen[column] {
			return errors.New("invalid selected columns")
		}
		seen[column] = true
	}
	return nil
}
func (a Approved) Validate(region string) error {
	b, c := a.Binding, a.Control
	if !Identifier.MatchString(b.ConnectionID) || b.Revision != 1 || b.Region != region || c.ProtocolVersion != 1 || c.ConnectionID != b.ConnectionID || c.DesiredRevision == 0 || c.DesiredRevision > 1<<63-1 || c.ResyncRevision > c.DesiredRevision || c.AuthorityExpiresAt.IsZero() || (b.Mode != "snapshot" && b.Mode != "continuous") || (c.DesiredState != "running" && c.DesiredState != "paused" && c.DesiredState != "disconnected") {
		return errors.New("invalid connection identity or command")
	}
	if b.Source.Type != "postgresql" {
		return errors.New("unsupported connector type")
	}
	var selection PostgresSelection
	if Decode(bytes.NewReader(b.Source.Configuration), &selection) != nil || ValidateSelection(selection, b.Mode) != nil {
		return errors.New("invalid source configuration")
	}
	if b.Target.Version < 3 || (b.Target.SharedGateway && b.Target.ProjectID == "") {
		return errors.New("invalid target deployment identity")
	}
	if !CredentialReference(b.Source.CredentialRef, region) || !CredentialReference(b.Target.CredentialRef, region) || !Origin(b.Target.URL) || b.Target.Project == "" || !CollectionName.MatchString(b.Target.Collection) {
		return errors.New("invalid target or credential reference")
	}
	var indexes map[string]json.RawMessage
	if json.Unmarshal(b.IndexConfigs, &indexes) != nil || len(indexes) == 0 {
		return errors.New("invalid index configuration")
	}
	return nil
}
func (o Observation) Validate(id string) bool {
	if o.ProtocolVersion != 1 || o.ConnectionID != id || o.DesiredRevision == 0 || o.Sequence == 0 || o.Sequence > 1<<63-1 || o.CompletedResyncRevision > o.DesiredRevision {
		return false
	}
	switch o.Phase {
	case "initializing", "validating", "snapshotting", "applying", "running", "pausing", "paused", "completed", "disconnecting", "disconnected", "resyncing", "action_required", "expired":
	default:
		return false
	}
	switch o.ReasonCode {
	case "", "source_unreachable", "source_authentication_failed", "source_tls_failed", "source_setup_required", "source_schema_changed", "source_continuity_lost", "target_unavailable", "target_conflict", "state_volume_missing", "state_invalid", "authority_expired", "unsupported_source", "operation_failed":
	default:
		return false
	}
	return o.Position == nil || (o.Position.SourceType == "postgresql" && postgresOffset.MatchString(o.Position.Offset))
}
func Fingerprint(b Binding) string {
	raw, _ := json.Marshal(b)
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	_ = decoder.Decode(&value)
	raw, _ = json.Marshal(value)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func WorkerToken(master, region, id string) string {
	h := hmac.New(sha256.New, []byte(master))
	_, _ = h.Write([]byte("connection-worker-v1\x00" + region + "\x00" + id))
	return hex.EncodeToString(h.Sum(nil))
}
func HTTPClient() *http.Client {
	return &http.Client{Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}
