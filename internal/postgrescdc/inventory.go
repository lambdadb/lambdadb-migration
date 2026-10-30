package postgrescdc

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sort"
	"time"

	bolt "go.etcd.io/bbolt"
)

// InventoryInspection exposes metadata only. Known names are evidence to retain,
// not a claim that the inventory is complete or that a prefix proves ownership.
type InventoryInspection struct {
	Format                int               `json:"format"`
	BindingID             string            `json:"binding_id"`
	BindingFingerprint    string            `json:"binding_fingerprint"`
	DesiredRevision       uint64            `json:"desired_revision"`
	DesiredState          string            `json:"desired_state"`
	AuthorityExpiresAt    time.Time         `json:"authority_expires_at"`
	Epoch                 uint64            `json:"epoch"`
	KnownGenerations      []string          `json:"known_generations"`
	RowCount              int               `json:"row_count"`
	DurableLSN            uint64            `json:"durable_lsn,omitempty"`
	Reconciliation        *InventoryReceipt `json:"reconciliation,omitempty"`
	InventoryComplete     bool              `json:"inventory_complete"`
	StateDigest           string            `json:"state_digest"`
	ReconciliationAllowed bool              `json:"reconciliation_allowed"`
	BlockedReason         string            `json:"blocked_reason,omitempty"`
}

// InventoryReconciliation is a regional operator attestation, not remote
// discovery. The operator must independently verify every name and completeness.
// It changes no source or target resource and grants no capture/cleanup authority.
type InventoryReconciliation struct {
	Format                  int      `json:"format"`
	BindingID               string   `json:"binding_id"`
	ExpectedDesiredRevision uint64   `json:"expected_desired_revision"`
	StateDigest             string   `json:"state_digest"`
	OwnedGenerations        []string `json:"owned_generations"`
	OwnershipConfirmed      bool     `json:"ownership_confirmed"`
}

// InventoryReceipt makes an interrupted local repair reviewable and replayable.
type InventoryReceipt struct {
	DesiredRevision     uint64    `json:"desired_revision"`
	ReviewedStateDigest string    `json:"reviewed_state_digest"`
	ManifestDigest      string    `json:"manifest_digest"`
	ConfirmedAt         time.Time `json:"confirmed_at"`
}

// InspectInventory cannot create a missing file or change existing state. A
// running worker's exclusive bbolt lock excludes this offline inspection.
func InspectInventory(path string) (*InventoryInspection, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{ReadOnly: true, Timeout: 100 * time.Millisecond})
	if err != nil {
		return nil, errors.New("existing offline regional state required")
	}
	defer db.Close()
	var report *InventoryInspection
	err = db.View(func(tx *bolt.Tx) error { var err error; report, err = inspectInventory(tx); return err })
	return report, err
}

func inspectInventory(tx *bolt.Tx) (*InventoryInspection, error) {
	a, err := approved(tx)
	if err != nil {
		return nil, err
	}
	if a == nil || !controlID.MatchString(a.Binding.BindingID) {
		return nil, errors.New("cached managed binding required")
	}
	e, err := loadExecution(tx)
	if err != nil {
		return nil, err
	}
	l, err := loadLifecycle(tx)
	if err != nil {
		return nil, err
	}
	fp := a.Binding.fingerprint()
	if e.Owner.Epoch > 0 && e.Owner.Fingerprint != fp {
		return nil, errors.New("execution identity must be normalized by managed supervision")
	}
	if l != nil && l.Fingerprint != fp {
		return nil, errors.New("resource inventory identity changed")
	}
	r := &InventoryInspection{Format: 1, BindingID: a.Binding.BindingID, BindingFingerprint: fp, DesiredRevision: a.Control.DesiredRevision, DesiredState: a.Control.DesiredState, AuthorityExpiresAt: a.Control.AuthorityExpiresAt, Epoch: e.Owner.Epoch, KnownGenerations: []string{}}
	names := map[string]bool{}
	if l != nil {
		r.InventoryComplete = !l.LegacyUnknown
		r.Reconciliation = l.Reconciliation
		if receipt := l.Reconciliation; receipt != nil {
			if receipt.DesiredRevision == 0 || receipt.DesiredRevision > a.Control.DesiredRevision || receipt.ConfirmedAt.IsZero() || !validInventoryDigest(receipt.ManifestDigest) || !validInventoryDigest(receipt.ReviewedStateDigest) {
				return nil, errors.New("invalid inventory reconciliation receipt")
			}
		}
		for name := range l.Generations {
			names[name] = true
		}
	}
	if e.Owner.Name != "" {
		names[e.Owner.Name] = true
	}
	if e.Active != nil {
		names[e.Active.Generation] = true
	}
	for name := range names {
		if !validGenerationName(a.Binding, name) {
			return nil, errors.New("invalid recorded generation name")
		}
		r.KnownGenerations = append(r.KnownGenerations, name)
	}
	sort.Strings(r.KnownGenerations)
	if b := tx.Bucket([]byte("cdc")); b != nil {
		raw := b.Get([]byte("state"))
		var state State
		if len(raw) > MaxStateBytes || json.Unmarshal(raw, &state) != nil || state.Format != 1 || state.Fingerprint != fp || state.Rows == nil || len(state.Rows) > MaxRows {
			return nil, errors.New("invalid regional checkpoint")
		}
		r.RowCount = len(state.Rows)
		r.DurableLSN = state.LSN
	}
	if b := tx.Bucket([]byte("bootstrap")); b != nil {
		var owner bootstrap
		if json.Unmarshal(b.Get([]byte("owner")), &owner) != nil || owner.Fingerprint != fp {
			return nil, errors.New("invalid snapshot ownership")
		}
	}
	// Hash the exact recoverable state, including row bytes, without exporting any
	// source value. Ignore only reporting sequence, which cannot authorize repair.
	hash := sha256.New()
	for _, entry := range []struct{ bucket, key string }{{"managed_control", "approved"}, {"execution", "state"}, {"cdc", "state"}, {"bootstrap", "owner"}, {"lifecycle", "state"}} {
		raw := []byte(nil)
		if b := tx.Bucket([]byte(entry.bucket)); b != nil {
			raw = b.Get([]byte(entry.key))
		}
		_, _ = hash.Write([]byte(entry.bucket + "\x00"))
		_, _ = hash.Write(binary.AppendUvarint(nil, uint64(len(raw))))
		_, _ = hash.Write(raw)
	}
	r.StateDigest = hex.EncodeToString(hash.Sum(nil))
	switch {
	case l != nil && l.NameCollision:
		r.BlockedReason = "generation_collision_requires_manual_resolution"
	case len(r.KnownGenerations) > maxTrackedGenerations:
		r.BlockedReason = "inventory_budget_exceeded"
	case r.InventoryComplete:
		r.BlockedReason = "inventory_already_complete"
	case a.Control.DesiredState != "paused" && a.Control.DesiredState != "removed":
		r.BlockedReason = "pause_or_removal_required"
	case !time.Now().Before(a.Control.AuthorityExpiresAt):
		r.BlockedReason = "authority_expired"
	case e.Owner.Epoch > 0 && time.Now().Before(e.Owner.ExpiresAt):
		r.BlockedReason = "execution_authority_not_fenced"
	default:
		r.ReconciliationAllowed = true
	}
	return r, nil
}

func validInventoryDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

// ReconcileInventory repairs only a reviewed resource inventory under cached
// central pause/removal authority. It never starts a worker or contacts a source
// or target. Existing row/checkpoint/epoch/control bytes remain unchanged.
func ReconcileInventory(path string, input io.Reader) (*InventoryInspection, error) {
	var request InventoryReconciliation
	raw, err := io.ReadAll(io.LimitReader(input, (16<<10)+1))
	if err != nil || len(raw) > 16<<10 {
		return nil, errors.New("invalid or oversized inventory manifest")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil {
		return nil, errors.New("invalid inventory manifest")
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF || request.Format != 1 || !request.OwnershipConfirmed || request.ExpectedDesiredRevision == 0 || request.OwnedGenerations == nil || len(request.OwnedGenerations) > maxTrackedGenerations || len(request.StateDigest) != 64 {
		return nil, errors.New("invalid or unconfirmed inventory manifest")
	}
	if _, err := hex.DecodeString(request.StateDigest); err != nil {
		return nil, errors.New("invalid inventory digest")
	}
	// Validate the existing database read-only before opening it for mutation.
	if _, err = InspectInventory(path); err != nil {
		return nil, err
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 100 * time.Millisecond, OpenFile: func(path string, flag int, mode os.FileMode) (*os.File, error) {
		return os.OpenFile(path, flag&^os.O_CREATE, mode)
	}})
	if err != nil {
		return nil, errors.New("existing offline regional state required")
	}
	defer db.Close()
	sort.Strings(request.OwnedGenerations)
	canonical, _ := json.Marshal(request)
	digest := sha256.Sum256(canonical)
	manifestDigest := hex.EncodeToString(digest[:])
	var result *InventoryInspection
	err = db.Update(func(tx *bolt.Tx) error {
		current, err := inspectInventory(tx)
		if err != nil {
			return err
		}
		if current.BindingID == request.BindingID && current.DesiredRevision == request.ExpectedDesiredRevision && current.InventoryComplete && current.Reconciliation != nil && current.Reconciliation.ManifestDigest == manifestDigest && current.Reconciliation.ReviewedStateDigest == request.StateDigest {
			result = current
			return nil
		}
		if !current.ReconciliationAllowed {
			return errors.New("regional state is not eligible for inventory reconciliation")
		}
		if current.BindingID != request.BindingID || current.DesiredRevision != request.ExpectedDesiredRevision || current.StateDigest != request.StateDigest {
			return errors.New("inventory review is stale or belongs to another binding")
		}
		a, err := approved(tx)
		if err != nil {
			return err
		}
		names := map[string]bool{}
		for _, name := range request.OwnedGenerations {
			if !validGenerationName(a.Binding, name) || names[name] {
				return errors.New("invalid or duplicate owned generation")
			}
			names[name] = true
		}
		for _, known := range current.KnownGenerations {
			if !names[known] {
				return errors.New("reviewed inventory omits a previously recorded generation")
			}
		}
		l, err := loadLifecycle(tx)
		if err != nil {
			return err
		}
		if l == nil {
			l = &lifecycleState{Format: 1, Fingerprint: a.Binding.fingerprint()}
		}
		l.Generations = names
		l.LegacyUnknown = false
		l.Reconciliation = &InventoryReceipt{DesiredRevision: current.DesiredRevision, ReviewedStateDigest: current.StateDigest, ManifestDigest: manifestDigest, ConfirmedAt: time.Now().UTC()}
		if err = saveLifecycle(tx, l); err != nil {
			return err
		}
		processBoundary("inventory_reconcile_write")
		result, err = inspectInventory(tx)
		return err
	})
	if err == nil {
		processBoundary("inventory_reconcile_committed")
	}
	return result, err
}
