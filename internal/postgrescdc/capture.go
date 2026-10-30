package postgrescdc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

// CaptureSelection contains PostgreSQL execution details only. No target generation,
// private project, search gate or central management call is part of source capture.
type CaptureSelection struct {
	ConnectionID string
	Region       string
	Schema       string
	Table        string
	Publication  string
	Columns      []string
}

func (c CaptureSelection) config() (Config, error) {
	if c.ConnectionID == "" || c.Region == "" || !name.MatchString(c.Schema) || !name.MatchString(c.Table) || !name.MatchString(c.Publication) {
		return Config{}, errors.New("invalid PostgreSQL capture selection")
	}
	hash := sha256.Sum256([]byte(c.ConnectionID))
	slot := "ldb_" + hex.EncodeToString(hash[:16])
	return Config{BindingID: c.ConnectionID, Revision: 1, TargetRegion: c.Region, WorkerRegion: c.Region, Schema: c.Schema, Table: c.Table, Publication: c.Publication, Slot: slot}, nil
}
func (c CaptureSelection) matches(columns []Column) bool {
	if len(c.Columns) == 0 {
		return true
	}
	if len(c.Columns) != len(columns) {
		return false
	}
	seen := map[string]bool{}
	for _, column := range c.Columns {
		if seen[column] {
			return false
		}
		seen[column] = true
	}
	for _, column := range columns {
		if !seen[column.Name] {
			return false
		}
	}
	return true
}

// Capture persists full row reconstruction state and committed source positions
// regionally. A target callback must complete durable ordered acceptance before
// returning; only then can Save and source feedback advance.
type Capture struct {
	Selection CaptureSelection
	Store     *Store
	Authorize func() error
}

func (c *Capture) Initialize(ctx context.Context, dsn string) (*State, error) {
	return c.initialize(ctx, dsn, true)
}

// Resume validates retained source state and never starts another baseline.
func (c *Capture) Resume(ctx context.Context, dsn string) (*State, error) {
	return c.initialize(ctx, dsn, false)
}
func (c *Capture) initialize(ctx context.Context, dsn string, allowNew bool) (*State, error) {
	config, err := c.Selection.config()
	if err != nil {
		return nil, err
	}
	if c.Store == nil {
		return nil, errors.New("regional source store required")
	}
	state, err := c.Store.Load()
	if err != nil {
		return nil, err
	}
	if state == nil && !allowNew {
		return nil, errors.New("source checkpoint missing; recovery required")
	}
	sql, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer sql.Close(context.Background())
	repl, err := replicationConn(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer repl.Close(context.Background())
	if state != nil {
		if err = validateResume(ctx, sql, repl, config, state); err != nil {
			return nil, err
		}
	} else {
		_, projection, e := preflight(ctx, sql, config)
		if e != nil {
			return nil, e
		}
		if !c.Selection.matches(projection) {
			return nil, errors.New("requested columns differ from publication projection")
		}
		state, err = snapshot(ctx, sql, repl, c.Store, config, nil)
		if err != nil {
			return nil, err
		}
		if !c.Selection.matches(state.Columns) {
			return nil, errors.New("requested columns differ from publication projection")
		}
		if err = c.Store.Save(state); err != nil {
			return nil, err
		}
	}
	if !c.Selection.matches(state.Columns) {
		return nil, errors.New("requested columns differ from publication projection")
	}
	return state, nil
}

// Resnapshot is an explicit operator-requested cut. It verifies the retained
// source identity and projection, but can replace a lost/invalid owned slot.
// The current checkpoint is untouched until PromotePreparedCut succeeds.
func (c *Capture) Resnapshot(ctx context.Context, dsn string) (*State, error) {
	config, err := c.Selection.config()
	if err != nil {
		return nil, err
	}
	old, err := c.Store.Load()
	if err != nil {
		return nil, err
	}
	if old == nil || old.Fingerprint != config.fingerprint() {
		return nil, errors.New("retained source ownership required for resync")
	}
	sql, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer sql.Close(context.Background())
	repl, err := replicationConn(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer repl.Close(context.Background())
	system, err := pglogrepl.IdentifySystem(ctx, repl)
	if err != nil {
		return nil, err
	}
	oid, cols, err := preflight(ctx, sql, config)
	if err != nil {
		return nil, err
	}
	if system.SystemID != old.SystemID || system.Timeline != old.Timeline || system.DBName != old.Database || oid != old.RelationID || !reflect.DeepEqual(cols, old.Columns) || !c.Selection.matches(cols) {
		return nil, errors.New("source identity or projection changed; new connection required")
	}
	if err = c.Store.saveBootstrap(bootstrap{Fingerprint: old.Fingerprint, SystemID: old.SystemID, Timeline: old.Timeline, Database: old.Database}); err != nil {
		return nil, err
	}
	return snapshot(ctx, sql, repl, c.Store, config, nil)
}

// ValidateCut checks that a prepared exported snapshot can still be followed by
// its exact logical stream. No target write should precede this check on restart.
func (c *Capture) ValidateCut(ctx context.Context, dsn string, state *State) error {
	config, err := c.Selection.config()
	if err != nil {
		return err
	}
	if state == nil || !c.Selection.matches(state.Columns) {
		return errors.New("source cut selection changed")
	}
	sql, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer sql.Close(context.Background())
	repl, err := replicationConn(ctx, dsn)
	if err != nil {
		return err
	}
	defer repl.Close(context.Background())
	return validateResume(ctx, sql, repl, config, state)
}

// ValidateRetained checks a retained decoded commit before its application plan
// finishes. The source identity and projection must match; a dropped slot is
// tolerated because the commit is already durable and streaming is re-validated
// by Resume or replaced by Resnapshot.
func (c *Capture) ValidateRetained(ctx context.Context, dsn string, state *State) error {
	config, err := c.Selection.config()
	if err != nil {
		return err
	}
	if state == nil || !c.Selection.matches(state.Columns) {
		return errors.New("source cut selection changed")
	}
	sql, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer sql.Close(context.Background())
	repl, err := replicationConn(ctx, dsn)
	if err != nil {
		return err
	}
	defer repl.Close(context.Background())
	return validateRetained(ctx, sql, repl, config, state)
}

func (c *Capture) Stream(ctx context.Context, dsn string, apply func(context.Context, *State, *State) error, ready func()) error {
	config, err := c.Selection.config()
	if err != nil {
		return err
	}
	if c.Store == nil || apply == nil {
		return errors.New("regional source store and target acceptance required")
	}
	state, err := c.Store.Load()
	if err != nil {
		return err
	}
	if state == nil {
		return errors.New("durable initial snapshot required before streaming")
	}
	sql, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer sql.Close(context.Background())
	repl, err := replicationConn(ctx, dsn)
	if err != nil {
		return err
	}
	defer repl.Close(context.Background())
	if err = validateResume(ctx, sql, repl, config, state); err != nil {
		return err
	}
	if !c.Selection.matches(state.Columns) {
		return errors.New("requested columns differ from publication projection")
	}
	if err = pglogrepl.StartReplication(ctx, repl, config.Slot, pglogrepl.LSN(state.LSN), pglogrepl.StartReplicationOptions{PluginArgs: []string{"proto_version '1'", "publication_names '" + config.Publication + "'", "messages 'true'"}}); err != nil {
		return err
	}
	if ready != nil {
		ready()
	}
	decoder := &transactionDecoder{state: state}
	nextFeedback := time.Now()
	nextHeartbeat := time.Now().Add(5 * time.Second)
	check := func() error {
		oid, columns, e := preflight(ctx, sql, config)
		if e != nil {
			return e
		}
		if oid != state.RelationID || !reflect.DeepEqual(columns, state.Columns) || !c.Selection.matches(columns) {
			return errors.New("source schema or selection changed")
		}
		if c.Authorize != nil {
			return c.Authorize()
		}
		return nil
	}
	for {
		if err = ctx.Err(); err != nil {
			return err
		}
		if !time.Now().Before(nextFeedback) {
			if err = check(); err != nil {
				return err
			}
			if !time.Now().Before(nextHeartbeat) {
				// A transactional logical message yields an ordered Commit even when the
				// selected table is quiet. Never acknowledge the SQL result or WAL end;
				// only the decoded commit may advance the durable checkpoint below.
				if _, err = sql.Exec(ctx, `SELECT pg_logical_emit_message(true,'lambdadb_capture_heartbeat','')`); err != nil {
					return errors.New("source heartbeat unavailable; check logical message permission")
				}
				nextHeartbeat = time.Now().Add(5 * time.Second)
				if c.Authorize != nil {
					if err = c.Authorize(); err != nil {
						return err
					}
				}
			}
			position := pglogrepl.LSN(state.LSN)
			if err = sendFeedback(ctx, repl, position); err != nil {
				return err
			}
			nextFeedback = time.Now().Add(time.Second)
		}
		receive, stop := context.WithDeadline(ctx, nextFeedback)
		message, e := repl.ReceiveMessage(receive)
		stop()
		if e != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !time.Now().Before(nextFeedback) {
				continue
			}
			return errors.New("source replication receive failed")
		}
		data, ok := message.(*pgproto3.CopyData)
		if !ok || len(data.Data) == 0 {
			return errors.New("unexpected source replication response")
		}
		switch data.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			keepalive, e := pglogrepl.ParsePrimaryKeepaliveMessage(data.Data[1:])
			if e != nil {
				return e
			}
			if keepalive.ReplyRequested {
				nextFeedback = time.Now()
			}
		case pglogrepl.XLogDataByteID:
			xlog, e := pglogrepl.ParseXLogData(data.Data[1:])
			if e != nil {
				return e
			}
			message, e := pglogrepl.Parse(xlog.WALData)
			if e != nil {
				return errors.New("invalid logical replication message")
			}
			next, e := decoder.consume(message, len(xlog.WALData))
			if e != nil {
				return e
			}
			if next == nil {
				continue
			}
			if e = check(); e != nil {
				return e
			}
			if e = apply(ctx, state, next); e != nil {
				return e
			}
			if c.Authorize != nil {
				if e = c.Authorize(); e != nil {
					return e
				}
			}
			if e = c.Store.Save(next); e != nil {
				return e
			}
			state = next
			decoder.state = state
			nextHeartbeat = time.Now().Add(5 * time.Second)
		default:
			return errors.New("unsupported source replication frame")
		}
	}
}

// Disconnect drops only the slot whose source identity and configuration were
// durably recorded by this capture. Publications and table data are preserved.
// Owned reports whether retained regional state claims a source slot or
// bootstrap, so a connection that never started can disconnect without a DSN.
func (c *Capture) Owned() (bool, error) {
	if c.Store == nil {
		return false, errors.New("regional source ownership state required")
	}
	state, err := c.Store.Load()
	if err != nil {
		return false, err
	}
	owner, err := c.Store.loadBootstrap()
	if err != nil {
		return false, err
	}
	return state != nil || owner != nil, nil
}

func (c *Capture) Disconnect(ctx context.Context, dsn string) error {
	config, err := c.Selection.config()
	if err != nil {
		return err
	}
	if c.Store == nil {
		return errors.New("regional source ownership state required")
	}
	state, err := c.Store.Load()
	if err != nil {
		return err
	}
	owner, err := c.Store.loadBootstrap()
	if err != nil {
		return err
	}
	if state == nil && owner == nil {
		return nil
	}
	sql, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer sql.Close(context.Background())
	var exists bool
	if err = sql.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)`, config.Slot).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return nil
	}
	repl, err := replicationConn(ctx, dsn)
	if err != nil {
		return err
	}
	defer repl.Close(context.Background())
	system, err := pglogrepl.IdentifySystem(ctx, repl)
	if err != nil {
		return err
	}
	if state != nil {
		if state.Fingerprint != config.fingerprint() || state.SystemID != system.SystemID || state.Timeline != system.Timeline || state.Database != system.DBName {
			return errors.New("source slot ownership changed")
		}
	}
	if owner != nil {
		if owner.Fingerprint != config.fingerprint() || owner.SystemID != system.SystemID || owner.Timeline != system.Timeline || owner.Database != system.DBName {
			return errors.New("source bootstrap ownership changed")
		}
	}
	var plugin, database string
	var active bool
	if err = sql.QueryRow(ctx, `SELECT plugin,database,active FROM pg_replication_slots WHERE slot_name=$1`, config.Slot).Scan(&plugin, &database, &active); err != nil {
		return err
	}
	if plugin != "pgoutput" || database != system.DBName || active {
		return errors.New("owned slot is active or its identity changed")
	}
	_, err = sql.Exec(ctx, `SELECT pg_drop_replication_slot($1)`, config.Slot)
	return err
}
