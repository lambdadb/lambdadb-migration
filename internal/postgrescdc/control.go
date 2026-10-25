package postgrescdc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

const maxControlBody = 64 << 10

var controlID = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,80}$`)
var ErrObsoleteControl = errors.New("obsolete regional control revision")
var ErrConflictingControl = errors.New("conflicting regional control revision")

type Command struct {
	ProtocolVersion    int       `json:"protocol_version"`
	BindingID          string    `json:"binding_id"`
	DesiredRevision    uint64    `json:"desired_revision"`
	DesiredState       string    `json:"desired_state"`
	AuthorityExpiresAt time.Time `json:"authority_expires_at"`
}
type Approved struct {
	Binding Config  `json:"binding"`
	Control Command `json:"control"`
}
type Observation struct {
	ProtocolVersion int    `json:"protocol_version"`
	BindingID       string `json:"binding_id"`
	DesiredRevision uint64 `json:"desired_revision"`
	Sequence        uint64 `json:"sequence"`
	Phase           string `json:"phase"`
	Epoch           uint64 `json:"epoch,omitempty"`
	Generation      string `json:"generation,omitempty"`
	DurableLSN      string `json:"durable_lsn,omitempty"`
	AppliedLSN      string `json:"applied_lsn,omitempty"`
	BarrierLSN      string `json:"barrier_lsn,omitempty"`
}

func (a *Approved) validate(id string) error {
	if err := a.Binding.ValidateIdentity(); err != nil {
		return err
	}
	c := a.Control
	if c.ProtocolVersion != 1 || c.BindingID != id || a.Binding.BindingID != id || c.DesiredRevision == 0 || c.DesiredRevision > math.MaxInt64 || (c.DesiredState != "running" && c.DesiredState != "paused") || c.AuthorityExpiresAt.IsZero() {
		return errors.New("invalid desired control identity")
	}
	return nil
}
func approved(tx *bolt.Tx) (*Approved, error) {
	b := tx.Bucket([]byte("managed_control"))
	if b == nil {
		return nil, nil
	}
	var a Approved
	if json.Unmarshal(b.Get([]byte("approved")), &a) != nil || a.validate(a.Binding.BindingID) != nil {
		return nil, errors.New("corrupt approved regional control")
	}
	return &a, nil
}
func (s *Store) Approved() (*Approved, error) {
	var a *Approved
	err := s.db.View(func(tx *bolt.Tx) error { var err error; a, err = approved(tx); return err })
	return a, err
}

// Upgrade only a fingerprint provably matching the original immutable bundle.
// All row images, LSNs, bootstrap ownership and generation epochs are retained.
func (s *Store) normalizeFingerprint(c Config) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		stable, legacy := c.fingerprint(), c.legacyFingerprint()
		for _, entry := range []struct{ bucket, key, field string }{{"cdc", "state", "fingerprint"}, {"bootstrap", "owner", "Fingerprint"}, {"execution", "state", ""}} {
			b := tx.Bucket([]byte(entry.bucket))
			if b == nil {
				continue
			}
			if entry.bucket == "execution" {
				e, err := loadExecution(tx)
				if err != nil {
					return err
				}
				if e.Owner.Fingerprint == stable {
					continue
				}
				if e.Owner.Fingerprint != legacy {
					return errors.New("binding identity changed; explicit resync required")
				}
				e.Owner.Fingerprint = stable
				if err = saveExecution(tx, e); err != nil {
					return err
				}
				continue
			}
			var fields map[string]json.RawMessage
			if json.Unmarshal(b.Get([]byte(entry.key)), &fields) != nil {
				return errors.New("corrupt recoverable state")
			}
			var fp string
			if json.Unmarshal(fields[entry.field], &fp) != nil {
				return errors.New("missing binding fingerprint")
			}
			if fp == stable {
				continue
			}
			if fp != legacy {
				return errors.New("binding identity changed; explicit resync required")
			}
			fields[entry.field], _ = json.Marshal(stable)
			data, err := json.Marshal(fields)
			if err != nil {
				return err
			}
			if err = b.Put([]byte(entry.key), data); err != nil {
				return err
			}
		}
		return nil
	})
}
func (s *Store) AcceptControl(a Approved, id string) error {
	if err := a.validate(id); err != nil {
		return err
	}
	if err := s.normalizeFingerprint(a.Binding); err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		previous, err := approved(tx)
		if err != nil {
			return err
		}
		if previous != nil {
			if previous.Binding.fingerprint() != a.Binding.fingerprint() {
				return ErrConflictingControl
			}
			if a.Control.DesiredRevision < previous.Control.DesiredRevision {
				return ErrObsoleteControl
			}
			if a.Control.DesiredRevision == previous.Control.DesiredRevision {
				before, _ := json.Marshal(previous.Control)
				after, _ := json.Marshal(a.Control)
				if !bytes.Equal(before, after) {
					return ErrConflictingControl
				}
				return nil
			}
		}
		b, err := tx.CreateBucketIfNotExists([]byte("managed_control"))
		if err != nil {
			return err
		}
		data, err := json.Marshal(a)
		if err != nil || len(data) > maxControlBody {
			return errors.New("invalid control size")
		}
		if err = b.Put([]byte("approved"), data); err != nil {
			return err
		}
		if tx.Bucket([]byte("execution")) != nil {
			e, err := loadExecution(tx)
			if err != nil {
				return err
			}
			if a.Control.DesiredState == "paused" {
				e.Owner.ExpiresAt = time.Now().UTC()
			} else if time.Now().Before(e.Owner.ExpiresAt) {
				e.Owner.ExpiresAt = a.Control.AuthorityExpiresAt
			}
			return saveExecution(tx, e)
		}
		return nil
	})
}
func managedAuthority(tx *bolt.Tx, run Generation) error {
	a, err := approved(tx)
	if err != nil {
		return err
	}
	if a == nil || a.Binding.fingerprint() != run.Fingerprint || a.Control.DesiredState != "running" || !time.Now().Before(a.Control.AuthorityExpiresAt) {
		return errors.New("managed execution authority unavailable")
	}
	e, err := loadExecution(tx)
	if err != nil {
		return err
	}
	return owned(e, run)
}
func (s *Store) checkManaged(run Generation) error {
	return s.db.View(func(tx *bolt.Tx) error { return managedAuthority(tx, run) })
}
func (s *Store) observationSequence() (uint64, error) {
	var seq uint64
	err := s.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("managed_control"))
		if err != nil {
			return err
		}
		if v := b.Get([]byte("sequence")); v != nil && json.Unmarshal(v, &seq) != nil {
			return errors.New("corrupt observation sequence")
		}
		if seq == math.MaxInt64 {
			return errors.New("observation sequence exhausted")
		}
		seq++
		data, _ := json.Marshal(seq)
		return b.Put([]byte("sequence"), data)
	})
	return seq, err
}

type ControlClient struct {
	URL, BindingID, Token string
	Client                *http.Client
}

func NewControlClient(raw, id, token string) (*ControlClient, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || !controlID.MatchString(id) || len(token) < 32 {
		return nil, errors.New("valid TLS regional control bootstrap required")
	}
	client := &http.Client{Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	configureProcessTestTLS(client)
	return &ControlClient{URL: strings.TrimRight(raw, "/"), BindingID: id, Token: token, Client: client}, nil
}
func (c *ControlClient) exchange(ctx context.Context, method, kind string, payload []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.URL+"/v1/bindings/"+c.BindingID+"/"+kind, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Content-Type", "application/json")
	response, err := c.Client.Do(req)
	if err != nil {
		return nil, errors.New("regional control unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, errors.New("regional control rejected request")
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxControlBody+1))
	if err != nil || len(data) > maxControlBody {
		return nil, errors.New("invalid regional response")
	}
	return data, nil
}
func (c *ControlClient) Fetch(ctx context.Context) (*Approved, error) {
	data, err := c.exchange(ctx, http.MethodGet, "desired", nil)
	if err != nil {
		return nil, err
	}
	var a Approved
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(&a) != nil || a.validate(c.BindingID) != nil {
		return nil, errors.New("invalid approved binding")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return nil, errors.New("trailing regional control data")
	}
	return &a, nil
}
func (c *ControlClient) Report(ctx context.Context, o Observation) error {
	data, err := json.Marshal(o)
	if err != nil {
		return err
	}
	_, err = c.exchange(ctx, http.MethodPut, "observed", data)
	return err
}

// Supervisor calls only the regional service. The last accepted command and
// status sequence survive restart even if both regional delivery and central API
// are unavailable. No source batch depends on a control HTTP request.
type Supervisor struct {
	Store    *Store
	Serving  *ServingGate
	Remote   *ControlClient
	Report   func(Progress)
	Interval time.Duration
}

func (s *Supervisor) Run(ctx context.Context) error {
	interval := s.Interval
	if interval == 0 {
		interval = time.Second
	}
	current, err := s.Store.Approved()
	if err != nil {
		return err
	}
	if fetched, e := s.Remote.Fetch(ctx); e == nil {
		if err = s.Store.AcceptControl(*fetched, s.Remote.BindingID); err == nil {
			current = fetched
		} else if !errors.Is(err, ErrObsoleteControl) && !errors.Is(err, ErrConflictingControl) {
			return err
		}
	}
	if current == nil {
		return errors.New("no recoverable approved regional configuration")
	}
	if current.Binding.BindingID != s.Remote.BindingID {
		return errors.New("cached binding does not match control bootstrap")
	}
	var cancel context.CancelFunc
	var done chan error
	stopRequested := false
	minimumEpoch := uint64(0)
	progress := make(chan Progress, 16)
	last := Progress{}
	phase := "starting"
	failedRevision := uint64(0)
	stopRun := func() {
		if cancel != nil {
			stopRequested = true
			cancel()
		}
	}
	defer func() {
		stopRun()
		if done != nil {
			<-done
		}
	}()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	reconcile := func() error {
		c := current.Control
		if c.DesiredState == "paused" || !time.Now().Before(c.AuthorityExpiresAt) {
			stopRun()
			if done != nil {
				phase = "pausing"
			} else if c.DesiredState == "paused" {
				phase = "paused"
			} else {
				phase = "expired"
			}
			return nil
		}
		if done == nil && failedRevision != c.DesiredRevision {
			dsn, key := os.Getenv(current.Binding.SourceDSNEnv), os.Getenv(current.Binding.TargetKeyEnv)
			if dsn == "" || key == "" {
				phase = "action_required"
				failedRevision = c.DesiredRevision
				return nil
			}
			execution, e := s.Store.Execution()
			if e != nil {
				return e
			}
			runCtx, stop := context.WithCancel(ctx)
			cancel = stop
			done = make(chan error, 1)
			binding := current.Binding
			binding.AuthorityExpiresAt = c.AuthorityExpiresAt
			minimumEpoch = execution.Owner.Epoch + 1
			stopRequested = false
			worker := &Worker{Config: binding, Store: s.Store, Target: NewLambdaTarget(binding, key), Managed: true, Serving: s.Serving, Report: func(p Progress) {
				select {
				case progress <- p:
				default:
				}
			}}
			completed := done
			phase = "starting"
			go func() { completed <- worker.Run(runCtx, dsn) }()
		}
		return nil
	}
	if err = reconcile(); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case p := <-progress:
			e, eerr := s.Store.Execution()
			if eerr != nil {
				return eerr
			}
			if p.Epoch < minimumEpoch || p.Epoch != e.Owner.Epoch || p.Generation != e.Owner.Name || !time.Now().Before(e.Owner.ExpiresAt) {
				continue
			}
			last = p
			if current.Control.DesiredState == "running" && time.Now().Before(current.Control.AuthorityExpiresAt) {
				phase = p.Phase
			}
			if s.Report != nil {
				s.Report(p)
			}
		case <-done:
			done = nil
			cancel = nil
			if !stopRequested && current.Control.DesiredState == "running" && time.Now().Before(current.Control.AuthorityExpiresAt) {
				failedRevision = current.Control.DesiredRevision
				phase = "action_required"
			}
			stopRequested = false
			if err = reconcile(); err != nil {
				return err
			}
		case <-ticker.C:
			if fetched, e := s.Remote.Fetch(ctx); e == nil {
				if err = s.Store.AcceptControl(*fetched, s.Remote.BindingID); err == nil {
					current = fetched
					if done != nil {
						e, err := s.Store.Execution()
						if err != nil {
							return err
						}
						if e.Owner.Epoch >= minimumEpoch && !time.Now().Before(e.Owner.ExpiresAt) {
							stopRun()
						}
					}
				} else if !errors.Is(err, ErrObsoleteControl) && !errors.Is(err, ErrConflictingControl) {
					return err
				}
			}
			if err = reconcile(); err != nil {
				return err
			}
			seq, e := s.Store.observationSequence()
			if e != nil {
				return e
			}
			_ = s.Remote.Report(ctx, Observation{ProtocolVersion: 1, BindingID: current.Binding.BindingID, DesiredRevision: current.Control.DesiredRevision, Sequence: seq, Phase: phase, Epoch: last.Epoch, Generation: last.Generation, DurableLSN: last.DurableLSN, AppliedLSN: last.AppliedLSN, BarrierLSN: last.BarrierLSN})
		}
	}
}
