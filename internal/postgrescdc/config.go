// Package postgrescdc implements a deliberately bounded PostgreSQL correctness spike.
package postgrescdc

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"regexp"
	"time"
)

// Config is an approved management-plane bundle, delivered to the worker region.
// It contains secret references (environment variable names), never credentials.
// Mapping changes require explicit resync; mutable authority uses managed control.
type Config struct {
	BindingID          string    `json:"binding_id"`
	Revision           int       `json:"revision"`
	TargetRegion       string    `json:"target_region"`
	WorkerRegion       string    `json:"worker_region,omitempty"`
	AuthorityExpiresAt time.Time `json:"authority_expires_at"`
	SourceDSNEnv       string    `json:"source_dsn_env"`
	TargetKeyEnv       string    `json:"target_key_env"`
	Schema             string    `json:"schema"`
	Table              string    `json:"table"`
	Publication        string    `json:"publication"`
	Slot               string    `json:"slot"`
	TargetURL          string    `json:"target_url"`
	TargetProject      string    `json:"target_project"`
	CollectionPrefix   string    `json:"collection_prefix"`
	// Raw schema uses the supported LambdaDB collection API shape.
	IndexConfigs json.RawMessage `json:"index_configs"`
}

var name = regexp.MustCompile(`^[a-z][a-z0-9_]{0,39}$`)
var prefix = regexp.MustCompile(`^cdc_[a-z0-9_]{1,16}$`)

func (c *Config) Validate() error {
	if err := c.ValidateIdentity(); err != nil {
		return err
	}
	if !time.Now().Before(c.AuthorityExpiresAt) {
		return errors.New("regional execution authority expired")
	}
	return nil
}

// ValidateIdentity excludes mutable control authority. Revision is the immutable
// binding/mapping revision, distinct from a control command's desired revision.
func (c *Config) ValidateIdentity() error {
	if c.WorkerRegion == "" {
		c.WorkerRegion = c.TargetRegion
	}
	if c.BindingID == "" || c.Revision < 1 || c.TargetRegion == "" || c.WorkerRegion != c.TargetRegion {
		return errors.New("binding/revision/target region required; cross-region worker placement is not supported")
	}
	for _, v := range []string{c.Schema, c.Table, c.Publication, c.Slot} {
		if !name.MatchString(v) {
			return errors.New("source identifiers must be lowercase simple names, at most 40 characters")
		}
	}
	if !prefix.MatchString(c.CollectionPrefix) || c.SourceDSNEnv == "" || c.TargetKeyEnv == "" || c.TargetProject == "" {
		return errors.New("secret references, target project and isolated cdc_ collection prefix required")
	}
	u, err := url.Parse(c.TargetURL)
	if err != nil || u.Host == "" || u.Scheme != "https" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return errors.New("target_url must be an HTTPS origin")
	}
	var indexes map[string]json.RawMessage
	if json.Unmarshal(c.IndexConfigs, &indexes) != nil || indexes == nil {
		return errors.New("index_configs must be an object")
	}
	return nil
}

func (c Config) fingerprint() string {
	if c.WorkerRegion == "" {
		c.WorkerRegion = c.TargetRegion
	}
	b, _ := json.Marshal(c)
	var fields map[string]any
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	_ = d.Decode(&fields)
	delete(fields, "authority_expires_at")
	b, _ = json.Marshal(fields)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func (c Config) legacyFingerprint() string {
	b, _ := json.Marshal(c)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
