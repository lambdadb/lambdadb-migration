package postgrescdc

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

const barrierPrefix = "lambdadb_cdc_barrier"

type Progress struct {
	BindingID        string `json:"binding_id"`
	ObservedRevision int    `json:"observed_revision"`
	WorkerRegion     string `json:"worker_region"`
	Generation       string `json:"generation"`
	Epoch            uint64 `json:"epoch"`
	BarrierLSN       string `json:"barrier_lsn,omitempty"`
	Phase            string `json:"phase"`
	DurableLSN       string `json:"durable_lsn"`
	AppliedLSN       string `json:"applied_lsn,omitempty"`
	// No searchable watermark: pending-inclusive Fetch is not a query-mode proof.
}

type Worker struct {
	Config  Config
	Store   *Store
	Target  Target
	Report  func(Progress)
	Serving *ServingGate
	Managed bool
	// Fault/barrier hooks are local test seams, never configuration or API fields.
	afterSnapshotImport func() error
	afterApply          func() error
	afterSave           func() error
}

// Run owns one binding until error/cancellation. A supervisor may restart it;
// each run must use the same regional state and creates a fresh target generation.
func (w *Worker) Run(ctx context.Context, dsn string) error {
	w.Serving.clear()
	defer w.Serving.clear()
	searchTarget, _ := w.Target.(SearchTarget)
	planner, _ := w.Target.(plannedTarget)
	c := w.Config
	if err := c.Validate(); err != nil {
		return err
	}
	w.Config = c
	if !w.Managed {
		cached, err := w.Store.Approved()
		if err != nil {
			return err
		}
		if cached != nil {
			return errors.New("managed regional state requires the control supervisor")
		}
	}
	var cancel context.CancelFunc
	if w.Managed {
		ctx, cancel = context.WithCancel(ctx)
	} else {
		ctx, cancel = context.WithDeadline(ctx, c.AuthorityExpiresAt)
	}
	defer cancel()
	sql, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return errors.New("source SQL connection failed")
	}
	defer sql.Close(context.Background())
	repl, err := replicationConn(ctx, dsn)
	if err != nil {
		return errors.New("source replication connection failed")
	}
	defer repl.Close(context.Background())
	if err = w.Store.normalizeFingerprint(c); err != nil {
		return err
	}
	state, err := w.Store.Load()
	if err != nil {
		return err
	}
	if state != nil {
		if err = validateResume(ctx, sql, repl, c, state); err != nil {
			return err
		}
	}
	run, err := w.Store.begin(c, w.Managed)
	if err != nil {
		return err
	}
	check := func() error {
		if w.Managed {
			return w.Store.checkManaged(run)
		}
		return ctx.Err()
	}
	if w.Managed {
		w.Target = &authorizedTarget{Target: w.Target, check: check}
	}
	save := func(state *State) error {
		if w.Managed {
			return w.Store.save(state, &run)
		}
		return w.Store.Save(state)
	}
	if state == nil {
		state, err = snapshot(ctx, sql, repl, w.Store, c, w.afterSnapshotImport)
		if err != nil {
			return err
		}
		if err = save(state); err != nil {
			return err
		}
	}
	var generation string
	if planner != nil {
		generation, err = planner.PlanGeneration(c)
		if err != nil {
			return err
		}
		run, err = w.Store.nameGeneration(run, generation)
		if err != nil {
			return err
		}
		processBoundary("generation_planned")
		if err = check(); err != nil {
			return err
		}
		err = planner.CreatePlanned(ctx, c, generation)
		if errors.Is(err, errGenerationCollision) {
			if e := w.Store.quarantineInventory(); e != nil {
				return e
			}
		}
		if err != nil {
			return err
		}
	} else {
		// Deterministic in-process test targets retain the existing create seam.
		generation, err = w.Target.Create(ctx, c)
		if err != nil {
			return err
		}
		run, err = w.Store.nameGeneration(run, generation)
		if err != nil {
			return err
		}
	}
	w.Serving.start(ctx, run, c, searchTarget)
	var barrierLSN string
	active := false
	report := func(phase string, applied bool) {
		if w.Report != nil {
			p := Progress{BindingID: c.BindingID, ObservedRevision: c.Revision, WorkerRegion: c.WorkerRegion, Generation: generation, Epoch: run.Epoch, BarrierLSN: barrierLSN, Phase: phase, DurableLSN: pglogrepl.LSN(state.LSN).String()}
			if applied {
				p.AppliedLSN = p.DurableLSN
			}
			w.Report(p)
		}
	}
	report("rebuilding", false)
	keys := SortedRowIDs(state.Rows)
	for _, id := range keys {
		if err = w.Target.Put(ctx, generation, id, state.Rows[id]); err != nil {
			return err
		}
		processBoundary("rebuild_row")
	}
	// This transactional message gives a stream-order barrier without locking or
	// rereading source rows. Its transaction must be decoded, applied and saved.
	var messagePosition string
	if err = sql.QueryRow(ctx, `SELECT pg_logical_emit_message(true,$1,$2)::text`, barrierPrefix, generation).Scan(&messagePosition); err != nil {
		return errors.New("source catch-up barrier could not be emitted")
	}
	messageLSN, err := pglogrepl.ParseLSN(messagePosition)
	if err != nil {
		return err
	}
	// A baseline is now recoverable; source-side WAL retained all concurrent work.
	if err = pglogrepl.StartReplication(ctx, repl, c.Slot, pglogrepl.LSN(state.LSN), pglogrepl.StartReplicationOptions{PluginArgs: []string{"proto_version '1'", "publication_names '" + c.Publication + "'", "messages 'true'"}}); err != nil {
		return err
	}
	report("streaming", true)
	decoder := &transactionDecoder{state: state, barrierToken: generation, barrierMessageLSN: messageLSN}
	nextFeedback := time.Now()
	for {
		if err = ctx.Err(); err != nil {
			return err
		}
		if err = check(); err != nil {
			return err
		}
		if !time.Now().Before(nextFeedback) {
			// Recheck selection/DDL even during idle periods. Never acknowledge a
			// keepalive's WAL end or an uncommitted transaction.
			oid, cols, e := preflight(ctx, sql, c)
			if e != nil {
				return e
			}
			if oid != state.RelationID || !reflect.DeepEqual(cols, state.Columns) {
				return errors.New("source schema drift; action required")
			}
			position := pglogrepl.LSN(state.LSN)
			if err = sendFeedback(ctx, repl, position); err != nil {
				return err
			}
			w.Serving.healthy(run)
			nextFeedback = time.Now().Add(time.Second)
		}
		receiveCtx, receiveCancel := context.WithDeadline(ctx, nextFeedback)
		message, e := repl.ReceiveMessage(receiveCtx)
		receiveCancel()
		if e != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if time.Now().After(nextFeedback) {
				continue
			}
			return errors.New("replication receive failed; restart from regional checkpoint")
		}
		if _, ok := message.(*pgproto3.ErrorResponse); ok {
			return errors.New("source replication error; continuity must be checked")
		}
		data, ok := message.(*pgproto3.CopyData)
		if !ok {
			return errors.New("unexpected replication response")
		}
		if len(data.Data) == 0 {
			return errors.New("empty replication response")
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
			msg, e := pglogrepl.Parse(xlog.WALData)
			if e != nil {
				return errors.New("invalid logical replication message")
			}
			next, e := decoder.consume(msg, len(xlog.WALData))
			if e != nil {
				return e
			}
			if next == nil {
				continue
			}
			// Do not send any target mutations for rejected DDL/publication drift.
			oid, cols, e := preflight(ctx, sql, c)
			if e != nil {
				return e
			}
			if oid != state.RelationID || !reflect.DeepEqual(cols, state.Columns) {
				return errors.New("source schema drift; action required")
			}
			changed := !equalJSON(state.Rows, next.Rows)
			if changed {
				processBoundary("before_apply")
			}
			w.Serving.clear()
			if err = applyRows(ctx, w.Target, generation, state.Rows, next.Rows); err != nil {
				return err
			}
			if changed && w.afterApply != nil {
				if err = w.afterApply(); err != nil {
					return err
				}
			}
			if changed {
				processBoundary("after_apply")
			}
			if err = save(next); err != nil {
				return err
			}
			state = next
			decoder.state = state
			if changed {
				processBoundary("after_save")
			}
			if changed && w.afterSave != nil {
				if err = w.afterSave(); err != nil {
					return err
				}
			}
			if decoder.committedBarrier {
				barrierLSN = pglogrepl.LSN(state.LSN).String()
				report("verifying", true)
				if err = verifyWithFeedback(ctx, w.Target, generation, state.Rows, func() error {
					position := pglogrepl.LSN(state.LSN)
					return sendFeedback(ctx, repl, position)
				}); err != nil {
					return err
				}
				if err = ctx.Err(); err != nil {
					return err
				}
				oid, cols, e := preflight(ctx, sql, c)
				if e != nil {
					return e
				}
				if oid != state.RelationID || !reflect.DeepEqual(cols, state.Columns) {
					return errors.New("source drift during generation verification")
				}
				digest, e := rowsDigest(state.Rows)
				if e != nil {
					return e
				}
				if err = w.Store.recordVerified(run, state.LSN, digest); err != nil {
					return err
				}
				processBoundary("before_activate")
				if _, err = w.Store.activate(run, run.ExpectedActiveEpoch); err != nil {
					return err
				}
				processBoundary("after_activate")
				active = true
			}
			if active {
				w.Serving.ready(run)
			}
			phase := "streaming"
			if active {
				phase = "active"
			}
			report(phase, true)
		default:
			return errors.New("unsupported replication frame")
		}
	}
}

func verifyWithFeedback(ctx context.Context, target Target, generation string, rows map[string]Row, feedback func() error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- target.Verify(ctx, generation, rows) }()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case err := <-done:
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := feedback(); err != nil {
				return errors.New("source connection lost during generation verification")
			}
		}
	}
}

// SortedRowIDs returns deterministic document operation order.
func SortedRowIDs(rows map[string]Row) []string {
	keys := make([]string, 0, len(rows))
	for id := range rows {
		keys = append(keys, id)
	}
	sort.Strings(keys)
	return keys
}
func applyRows(ctx context.Context, target Target, generation string, previous, next map[string]Row) error {
	// Full new row images first: PK changes never delete the only recoverable
	// image. Both this image and the source transaction remain recoverable.
	for _, id := range SortedRowIDs(next) {
		if !equalJSON(previous[id], next[id]) {
			if err := target.Put(ctx, generation, id, next[id]); err != nil {
				return err
			}
		}
	}
	for _, id := range SortedRowIDs(previous) {
		if _, ok := next[id]; !ok {
			if err := target.Delete(ctx, generation, id); err != nil {
				return err
			}
		}
	}
	return nil
}

type transactionDecoder struct {
	state                *State
	rows                 map[string]Row
	bytes                int
	active               bool
	relationSeen         bool
	duplicate            bool
	barrierToken         string
	barrierMessageLSN    pglogrepl.LSN
	barrierInTransaction bool
	committedBarrier     bool
}

func (d *transactionDecoder) consume(msg pglogrepl.Message, size int) (*State, error) {
	if d.duplicate {
		switch msg.(type) {
		case *pglogrepl.CommitMessage:
			d.duplicate = false
			d.active = false
			return nil, nil
		case *pglogrepl.RelationMessage:
			// Keep relation validation for the following non-duplicate transaction.
		default:
			return nil, nil
		}
	}
	if d.active {
		d.bytes += size
		if d.bytes > MaxTransactionBytes {
			return nil, errors.New("transaction budget exceeded; no acknowledgement")
		}
	}
	switch m := msg.(type) {
	case *pglogrepl.BeginMessage:
		if d.active {
			return nil, errors.New("nested transaction")
		}
		d.active = true
		d.barrierInTransaction = false
		d.committedBarrier = false
		// FinalLSN is the commit-record LSN; checkpoint is its end position.
		if uint64(m.FinalLSN) < d.state.LSN {
			d.duplicate = true
		}
		d.rows = cloneRows(d.state.Rows)
		d.bytes = 0
	case *pglogrepl.RelationMessage:
		if m.RelationID != d.state.RelationID || m.Namespace == "" || len(m.Columns) != len(d.state.Columns) || m.ReplicaIdentity != 'd' {
			return nil, errors.New("relation drift; action required")
		}
		for i, c := range d.state.Columns {
			mc := m.Columns[i]
			if mc.Name != c.Name || mc.DataType != c.OID || mc.TypeModifier != c.Modifier || (mc.Flags&1 != 0) != c.Key {
				return nil, errors.New("relation columns changed; action required")
			}
		}
		d.relationSeen = true
	case *pglogrepl.InsertMessage:
		if err := d.requireRow(m.RelationID); err != nil {
			return nil, err
		}
		row, err := d.tuple(m.Tuple, nil, false)
		if err != nil {
			return nil, err
		}
		id, err := rowID(row, d.state.Columns)
		if err != nil {
			return nil, err
		}
		d.rows[id] = row
	case *pglogrepl.UpdateMessage:
		if err := d.requireRow(m.RelationID); err != nil {
			return nil, err
		}
		keyTuple := m.NewTuple
		if m.OldTuple != nil {
			keyTuple = m.OldTuple
		}
		key, err := d.tuple(keyTuple, nil, true)
		if err != nil {
			return nil, err
		}
		oldID, err := rowID(key, d.state.Columns)
		if err != nil {
			return nil, err
		}
		prior, ok := d.rows[oldID]
		if !ok {
			return nil, errors.New("update has no recoverable prior row")
		}
		row, err := d.tuple(m.NewTuple, prior, false)
		if err != nil {
			return nil, err
		}
		id, err := rowID(row, d.state.Columns)
		if err != nil {
			return nil, err
		}
		delete(d.rows, oldID)
		d.rows[id] = row
	case *pglogrepl.DeleteMessage:
		if err := d.requireRow(m.RelationID); err != nil {
			return nil, err
		}
		key, err := d.tuple(m.OldTuple, nil, true)
		if err != nil {
			return nil, err
		}
		id, err := rowID(key, d.state.Columns)
		if err != nil {
			return nil, err
		}
		if _, ok := d.rows[id]; !ok {
			return nil, errors.New("delete has no recoverable prior row")
		}
		delete(d.rows, id)
	case *pglogrepl.CommitMessage:
		if !d.active {
			return nil, errors.New("commit outside transaction")
		}
		d.active = false
		if uint64(m.TransactionEndLSN) <= d.state.LSN {
			d.rows = nil
			return nil, nil
		}
		if m.TransactionEndLSN <= m.CommitLSN {
			return nil, errors.New("invalid transaction boundary")
		}
		if len(d.rows) > MaxRows {
			return nil, errors.New("row budget exceeded")
		}
		next := *d.state
		next.Rows = d.rows
		next.LSN = uint64(m.TransactionEndLSN)
		d.rows = nil
		d.committedBarrier = d.barrierInTransaction
		return &next, nil
	case *pglogrepl.LogicalDecodingMessage:
		if m.Prefix == barrierPrefix && d.barrierToken != "" && string(m.Content) == d.barrierToken {
			if !d.active || !m.Transactional || m.LSN != d.barrierMessageLSN {
				return nil, errors.New("invalid catch-up barrier message")
			}
			d.barrierInTransaction = true
		}
	case *pglogrepl.TruncateMessage:
		return nil, errors.New("TRUNCATE requires explicit resync")
	case *pglogrepl.TypeMessage, *pglogrepl.OriginMessage:
		// Metadata only; unsupported actual column types fail preflight.
	default:
		return nil, fmt.Errorf("unsupported logical message %T", msg)
	}
	return nil, nil
}
func (d *transactionDecoder) requireRow(relation uint32) error {
	if !d.active || !d.relationSeen || relation != d.state.RelationID {
		return errors.New("row outside validated transaction/relation")
	}
	return nil
}
func (d *transactionDecoder) tuple(tuple *pglogrepl.TupleData, prior Row, keyOnly bool) (Row, error) {
	if tuple == nil || len(tuple.Columns) != len(d.state.Columns) {
		return nil, errors.New("tuple shape changed")
	}
	row := Row{}
	for i, col := range d.state.Columns {
		if keyOnly && !col.Key {
			continue
		}
		value := tuple.Columns[i]
		switch value.DataType {
		case 'n':
			row[col.Name] = []byte("null")
		case 'u':
			v, ok := prior[col.Name]
			if !ok {
				return nil, errors.New("unchanged TOAST has no durable prior image")
			}
			row[col.Name] = v
		case 't':
			v, err := decodeValue(col.OID, value.Data)
			if err != nil {
				return nil, err
			}
			row[col.Name] = v
		default:
			return nil, errors.New("unsupported tuple encoding")
		}
	}
	return row, nil
}
