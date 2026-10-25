package postgrescdc

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	bolt "go.etcd.io/bbolt"
)

const maxTrackedGenerations = 128

type lifecycleState struct {
	Format                  int               `json:"format"`
	Fingerprint             string            `json:"fingerprint"`
	Generations             map[string]bool   `json:"generations"`
	NameCollision           bool              `json:"name_collision,omitempty"`
	Reconciliation          *InventoryReceipt `json:"reconciliation,omitempty"`
	LegacyUnknown           bool              `json:"legacy_unknown,omitempty"`
	CompletedResyncRevision uint64            `json:"completed_resync_revision,omitempty"`
	Removed                 bool              `json:"removed,omitempty"`
}

func loadLifecycle(tx *bolt.Tx) (*lifecycleState, error) {
	b := tx.Bucket([]byte("lifecycle"))
	if b == nil {
		return nil, nil
	}
	raw := b.Get([]byte("state"))
	var l lifecycleState
	if len(raw) > 64<<10 || json.Unmarshal(raw, &l) != nil || l.Format != 1 || l.Fingerprint == "" || l.Generations == nil || len(l.Generations) > maxTrackedGenerations {
		return nil, errors.New("corrupt regional resource inventory")
	}
	for name := range l.Generations {
		if name == "" || len(name) > 80 {
			return nil, errors.New("invalid resource inventory name")
		}
	}
	return &l, nil
}
func saveLifecycle(tx *bolt.Tx, l *lifecycleState) error {
	b, err := tx.CreateBucketIfNotExists([]byte("lifecycle"))
	if err != nil {
		return err
	}
	raw, err := json.Marshal(l)
	if err != nil {
		return err
	}
	return b.Put([]byte("state"), raw)
}
func initLifecycle(tx *bolt.Tx, c Config, e *Execution) error {
	l, err := loadLifecycle(tx)
	if err != nil {
		return err
	}
	if l != nil {
		if l.Fingerprint != c.fingerprint() {
			return errors.New("resource inventory identity changed")
		}
		return nil
	}
	l = &lifecycleState{Format: 1, Fingerprint: c.fingerprint(), Generations: map[string]bool{}}
	if e.Owner.Epoch > 0 || tx.Bucket([]byte("cdc")) != nil || tx.Bucket([]byte("bootstrap")) != nil {
		// Old state did not record every create intent. Never infer ownership by
		// listing all collections with a prefix and deleting apparent matches.
		l.LegacyUnknown = true
		if e.Owner.Name != "" {
			l.Generations[e.Owner.Name] = true
		}
		if e.Active != nil {
			l.Generations[e.Active.Generation] = true
		}
	}
	return saveLifecycle(tx, l)
}
func trackGeneration(tx *bolt.Tx, run Generation, name string) error {
	l, err := loadLifecycle(tx)
	if err != nil {
		return err
	}
	if l == nil || l.Fingerprint != run.Fingerprint || l.Removed {
		return errors.New("resource inventory unavailable")
	}
	l.Generations[name] = true
	if len(l.Generations) > maxTrackedGenerations {
		return errors.New("generation inventory budget exceeded; explicit cleanup required")
	}
	return saveLifecycle(tx, l)
}
func (s *Store) lifecycle() (*lifecycleState, error) {
	var l *lifecycleState
	err := s.db.View(func(tx *bolt.Tx) error { var err error; l, err = loadLifecycle(tx); return err })
	return l, err
}
func (s *Store) quarantineInventory() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		l, err := loadLifecycle(tx)
		if err != nil {
			return err
		}
		if l == nil {
			return errors.New("missing inventory")
		}
		l.LegacyUnknown = true
		l.NameCollision = true
		return saveLifecycle(tx, l)
	})
}
func lifecyclePending(c Command, l *lifecycleState) bool {
	return c.DesiredState == "removed" && (l == nil || !l.Removed) || c.DesiredState == "running" && c.ResyncRevision > 0 && (l == nil || l.CompletedResyncRevision < c.ResyncRevision)
}
func cleanupAuthority(tx *bolt.Tx, wanted Approved) error {
	a, err := approved(tx)
	if err != nil {
		return err
	}
	if a == nil || a.Binding.fingerprint() != wanted.Binding.fingerprint() || a.Control.ProtocolVersion != 2 || a.Control.DesiredState != wanted.Control.DesiredState || a.Control.ResyncRevision != wanted.Control.ResyncRevision || !time.Now().Before(a.Control.AuthorityExpiresAt) {
		return errors.New("cleanup authority changed or expired")
	}
	if a.Control.DesiredState != "removed" && (a.Control.DesiredState != "running" || a.Control.ResyncRevision == 0) {
		return errors.New("cleanup not requested")
	}
	return nil
}
func (s *Store) checkCleanup(a Approved) error {
	return s.db.View(func(tx *bolt.Tx) error { return cleanupAuthority(tx, a) })
}

// cleanup is called only after the capture goroutine has exited. The regional
// file lock excludes another process; every remote mutation rechecks authority.
func (s *Store) cleanup(ctx context.Context, a Approved, dsn string, target *LambdaTarget) error {
	if err := s.db.Update(func(tx *bolt.Tx) error {
		if err := cleanupAuthority(tx, a); err != nil {
			return err
		}
		e, err := loadExecution(tx)
		if err != nil {
			return err
		}
		return initLifecycle(tx, a.Binding, e)
	}); err != nil {
		return err
	}
	l, err := s.lifecycle()
	if err != nil {
		return err
	}
	if l.LegacyUnknown {
		return errors.New("legacy generation ownership is incomplete; manual inventory required")
	}
	if !lifecyclePending(a.Control, l) {
		return nil
	}
	if err = s.checkCleanup(a); err != nil {
		return err
	}
	if err = s.dropOwnedSlot(ctx, a, dsn); err != nil {
		return err
	}
	names := make([]string, 0, len(l.Generations))
	for name := range l.Generations {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err = s.checkCleanup(a); err != nil {
			return err
		}
		if err = target.RemoveGeneration(ctx, a.Binding, name); err != nil {
			return err
		}
		processBoundary("cleanup_target_deleted")
		if err = s.db.Update(func(tx *bolt.Tx) error {
			if err := cleanupAuthority(tx, a); err != nil {
				return err
			}
			current, err := loadLifecycle(tx)
			if err != nil {
				return err
			}
			delete(current.Generations, name)
			return saveLifecycle(tx, current)
		}); err != nil {
			return err
		}
	}
	processBoundary("cleanup_before_reset")
	err = s.db.Update(func(tx *bolt.Tx) error {
		if err := cleanupAuthority(tx, a); err != nil {
			return err
		}
		current, err := loadLifecycle(tx)
		if err != nil {
			return err
		}
		if current == nil || current.LegacyUnknown || len(current.Generations) != 0 {
			return errors.New("cleanup inventory is incomplete")
		}
		for _, name := range []string{"cdc", "bootstrap"} {
			if tx.Bucket([]byte(name)) != nil {
				if err := tx.DeleteBucket([]byte(name)); err != nil {
					return err
				}
			}
		}
		if tx.Bucket([]byte("execution")) != nil {
			e, err := loadExecution(tx)
			if err != nil {
				return err
			}
			// Epochs never regress, even though explicit resync discards the old baseline.
			e.Active = nil
			e.Owner.Name = ""
			e.Owner.ExpectedActiveEpoch = 0
			e.Owner.VerifiedLSN = 0
			e.Owner.VerifiedDigest = ""
			e.Owner.ExpiresAt = time.Now().UTC()
			if err = saveExecution(tx, e); err != nil {
				return err
			}
		}
		if a.Control.DesiredState == "removed" {
			current.Removed = true
		} else {
			current.CompletedResyncRevision = a.Control.ResyncRevision
		}
		if err = saveLifecycle(tx, current); err != nil {
			return err
		}
		processBoundary("cleanup_reset_write")
		return nil
	})
	if err == nil {
		processBoundary("cleanup_reset_committed")
	}
	return err
}
func (s *Store) dropOwnedSlot(ctx context.Context, a Approved, dsn string) error {
	sql, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return errors.New("cleanup source connection unavailable")
	}
	defer sql.Close(context.Background())
	state, err := s.Load()
	if err != nil {
		return err
	}
	boot, err := s.loadBootstrap()
	if err != nil {
		return err
	}
	if state != nil || boot != nil {
		repl, err := replicationConn(ctx, dsn)
		if err != nil {
			return errors.New("cleanup source identity unavailable")
		}
		defer repl.Close(context.Background())
		system, err := pglogrepl.IdentifySystem(ctx, repl)
		if err != nil {
			return errors.New("cleanup source identity unavailable")
		}
		owner := boot
		if state != nil {
			owner = &bootstrap{Fingerprint: state.Fingerprint, SystemID: state.SystemID, Timeline: state.Timeline, Database: state.Database}
		}
		if owner.Fingerprint != a.Binding.fingerprint() || owner.SystemID != system.SystemID || owner.Timeline != system.Timeline || owner.Database != system.DBName {
			return errors.New("cleanup source ownership changed")
		}
		if a.Control.DesiredState == "running" && state != nil {
			oid, cols, err := preflight(ctx, sql, a.Binding)
			if err != nil {
				return err
			}
			if oid != state.RelationID || !reflect.DeepEqual(cols, state.Columns) {
				return errors.New("resync cannot repair incompatible source schema")
			}
		}
	}
	var plugin, database string
	var active bool
	err = sql.QueryRow(ctx, "SELECT plugin,database,active FROM pg_replication_slots WHERE slot_name=$1", a.Binding.Slot).Scan(&plugin, &database, &active)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return errors.New("cleanup slot inspection failed")
	}
	owner := boot
	if state != nil {
		owner = &bootstrap{Database: state.Database}
	}
	if owner == nil || plugin != "pgoutput" || database != owner.Database || active {
		return errors.New("slot is active or not provably owned; refusing cleanup")
	}
	if err = s.checkCleanup(a); err != nil {
		return err
	}
	if _, err = sql.Exec(ctx, "SELECT pg_drop_replication_slot($1)", a.Binding.Slot); err != nil {
		return errors.New("owned slot cleanup failed")
	}
	processBoundary("cleanup_slot_dropped")
	return nil
}
