package postgrescdc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func replicationConn(ctx context.Context, dsn string) (*pgconn.PgConn, error) {
	c, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("invalid source connection configuration")
	}
	c.RuntimeParams["replication"] = "database"
	return pgconn.ConnectConfig(ctx, c)
}

type sourceQuery interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}
type tableColumn struct {
	Column
	Generated string
}

// Read catalog metadata in one statement so each validation has one catalog
// snapshot and one network round trip, including transaction-time drift checks.
const tableInspectionFields = `current_setting('server_version_num')::int,
 c.oid,c.relkind::text,c.relreplident::text,c.relrowsecurity,c.relispartition,
 (SELECT jsonb_agg(jsonb_build_object('Name',a.attname,'OID',a.atttypid::bigint,
 'Modifier',a.atttypmod,'Key',COALESCE(a.attnum=ANY(i.indkey[0:i.indnkeyatts-1]),false),
 'Generated',a.attgenerated::text) ORDER BY a.attnum)
 FROM pg_attribute a LEFT JOIN pg_index i ON i.indrelid=a.attrelid AND i.indisprimary
 WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped)`
const tableInspectionFrom = ` FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname=$1 AND c.relname=$2`

type tableInspection struct {
	version        int
	oid            uint32
	kind, identity string
	rls, partition bool
	columns        []tableColumn
}

func (t *tableInspection) destinations() []any {
	return []any{&t.version, &t.oid, &t.kind, &t.identity, &t.rls, &t.partition, &t.columns}
}
func (t *tableInspection) validate(continuous bool) error {
	if t.version < 150000 {
		return errors.New("PostgreSQL 15+ required")
	}
	if t.kind != "r" || (continuous && t.identity != "d") || t.rls || t.partition {
		return errors.New("ordinary non-RLS, non-partitioned table with default replica identity required")
	}
	keys := 0
	for _, col := range t.columns {
		if col.Key {
			keys++
		}
	}
	if keys != 1 {
		return errors.New("single-column primary key required")
	}
	return nil
}
func inspectTable(ctx context.Context, conn sourceQuery, c Config, continuous bool) (uint32, []tableColumn, error) {
	var table tableInspection
	if err := conn.QueryRow(ctx, "SELECT "+tableInspectionFields+tableInspectionFrom, c.Schema, c.Table).Scan(table.destinations()...); err != nil {
		if ctx.Err() != nil {
			return 0, nil, ctx.Err()
		}
		return 0, nil, err
	}
	if err := table.validate(continuous); err != nil {
		return 0, nil, err
	}
	return table.oid, table.columns, nil
}

func preflight(ctx context.Context, conn *pgx.Conn, c Config) (uint32, []Column, error) {
	var table tableInspection
	var publication *struct{ All, Insert, Update, Delete, Truncate bool }
	var selections []struct {
		Schema, Table string
		Columns       []string
		Filter        *string
	}
	// Two entries suffice to reject a multi-table publication without aggregating it.
	query := "SELECT " + tableInspectionFields + `,
 (SELECT jsonb_build_object('All',puballtables,'Insert',pubinsert,'Update',pubupdate,
 'Delete',pubdelete,'Truncate',pubtruncate) FROM pg_publication WHERE pubname=$3),
 (SELECT jsonb_agg(jsonb_build_object('Schema',schemaname,'Table',tablename,
 'Columns',attnames,'Filter',rowfilter)) FROM
 (SELECT schemaname,tablename,attnames,rowfilter FROM pg_publication_tables WHERE pubname=$3 LIMIT 2) selected)` + tableInspectionFrom
	destinations := append(table.destinations(), &publication, &selections)
	if err := conn.QueryRow(ctx, query, c.Schema, c.Table, c.Publication).Scan(destinations...); err != nil {
		if ctx.Err() != nil {
			return 0, nil, ctx.Err()
		}
		return 0, nil, err
	}
	if err := table.validate(true); err != nil {
		return 0, nil, err
	}
	if publication == nil {
		return 0, nil, pgx.ErrNoRows
	}
	if publication.All || !publication.Insert || !publication.Update || !publication.Delete || !publication.Truncate {
		return 0, nil, errors.New("dedicated publication with insert/update/delete/truncate required")
	}
	for _, selected := range selections {
		if selected.Schema != c.Schema || selected.Table != c.Table || selected.Filter != nil || len(selected.Columns) == 0 {
			return 0, nil, errors.New("publication selection mismatch")
		}
	}
	if len(selections) != 1 {
		return 0, nil, errors.New("publication must select exactly one table")
	}
	cols, err := selectColumns(table.columns, selections[0].Columns)
	return table.oid, cols, err
}

func selectColumns(tableColumns []tableColumn, published []string) ([]Column, error) {
	// pgoutput emits selected columns in table attribute order, independent of
	// the order in CREATE PUBLICATION. Use that exact projection for snapshot,
	// row reconstruction and drift checks. Unpublished types/values are irrelevant.
	selected := make(map[string]bool, len(published))
	for _, column := range published {
		if selected[column] {
			return nil, errors.New("duplicate publication column")
		}
		selected[column] = true
	}
	var cols []Column
	selectedKey := false
	for _, entry := range tableColumns {
		col := entry.Column
		if !selected[col.Name] {
			continue
		}
		if entry.Generated != "" {
			return nil, errors.New("selected generated columns unsupported")
		}
		if !supportedOID(col.OID) {
			return nil, fmt.Errorf("unsupported source type OID %d", col.OID)
		}
		if col.Key {
			selectedKey = true
			if col.OID != 20 && col.OID != 21 && col.OID != 23 && col.OID != 25 && col.OID != 1043 && col.OID != 2950 {
				return nil, errors.New("primary key must be integer, text or uuid")
			}
		}
		if col.Name == "id" && !col.Key {
			return nil, errors.New("non-key id column conflicts with target id")
		}
		cols = append(cols, col)
	}
	if len(cols) != len(selected) {
		return nil, errors.New("publication column mismatch")
	}
	if !selectedKey {
		return nil, errors.New("publication must include primary key")
	}
	return cols, nil
}

func supportedOID(oid uint32) bool {
	switch oid {
	case 16, 20, 21, 23, 25, 1043, 2950, 114, 3802, 700, 701:
		return true
	}
	return false
}
func decodeValue(oid uint32, value []byte) (json.RawMessage, error) {
	switch oid {
	case 25, 1043, 2950:
		return json.Marshal(string(value))
	case 16:
		if string(value) == "t" || string(value) == "true" {
			return json.RawMessage("true"), nil
		}
		if string(value) == "f" || string(value) == "false" {
			return json.RawMessage("false"), nil
		}
	case 20, 21, 23:
		if _, err := strconv.ParseInt(string(value), 10, 64); err == nil {
			return append(json.RawMessage(nil), value...), nil
		}
	case 700, 701:
		var number json.Number
		if json.Unmarshal(value, &number) == nil && string(value) != "null" {
			return append(json.RawMessage(nil), value...), nil
		}
	case 114, 3802:
		if json.Valid(value) {
			return append(json.RawMessage(nil), value...), nil
		}
	default:
		return nil, fmt.Errorf("unsupported type OID %d", oid)
	}
	return nil, errors.New("invalid or non-finite source value")
}

// snapshot persists the row baseline only after the whole exported snapshot.
// The durable bootstrap marker permits restart of a lost snapshot, with a fresh
// slot/snapshot pair. No target writes or source acknowledgements precede it.
func snapshot(ctx context.Context, sql *pgx.Conn, repl *pgconn.PgConn, store *Store, c Config, hook func() error) (*State, error) {
	oid, cols, err := preflight(ctx, sql, c)
	if err != nil {
		return nil, err
	}
	sys, err := pglogrepl.IdentifySystem(ctx, repl)
	if err != nil {
		return nil, err
	}
	owner := bootstrap{Fingerprint: c.fingerprint(), SystemID: sys.SystemID, Timeline: sys.Timeline, Database: sys.DBName}
	previous, err := store.loadBootstrap()
	if err != nil {
		return nil, err
	}
	var exists bool
	if err = sql.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)`, c.Slot).Scan(&exists); err != nil {
		return nil, err
	}
	if previous == nil {
		if exists {
			return nil, errors.New("slot already exists without regional ownership; refusing to adopt or drop")
		}
		if err = store.saveBootstrap(owner); err != nil {
			return nil, err
		}
	} else {
		if *previous != owner {
			return nil, errors.New("incomplete snapshot identity/configuration changed; resync required")
		}
		if exists {
			var plugin, db string
			var active bool
			if err = sql.QueryRow(ctx, `SELECT plugin,database,active FROM pg_replication_slots WHERE slot_name=$1`, c.Slot).Scan(&plugin, &db, &active); err != nil {
				return nil, err
			}
			if plugin != "pgoutput" || db != sys.DBName || active {
				return nil, errors.New("incomplete snapshot slot identity/ownership mismatch")
			}
			if _, err = sql.Exec(ctx, `SELECT pg_drop_replication_slot($1)`, c.Slot); err != nil {
				return nil, err
			}
		}
	}
	result, err := pglogrepl.CreateReplicationSlot(ctx, repl, c.Slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{SnapshotAction: "EXPORT_SNAPSHOT"})
	if err != nil {
		return nil, errors.New("cannot create owned slot; inspect bootstrap state and slot ownership")
	}
	tx, err := sql.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.Background())
	// PostgreSQL generated this token; quote it as a SQL literal anyway.
	if _, err = tx.Exec(ctx, "SET TRANSACTION SNAPSHOT '"+strings.ReplaceAll(result.SnapshotName, "'", "''")+"'"); err != nil {
		return nil, err
	}
	names := make([]string, len(cols))
	for i, col := range cols {
		names[i] = col.Name
	}
	if err = lockSnapshotProjection(ctx, tx, c.Schema, c.Table, names); err != nil {
		return nil, err
	}
	lockedOID, lockedCols, err := preflight(ctx, sql, c)
	if err != nil {
		return nil, err
	}
	if oid != lockedOID || !reflect.DeepEqual(cols, lockedCols) {
		return nil, errors.New("schema changed during snapshot setup")
	}
	if hook != nil {
		if err = hook(); err != nil {
			return nil, err
		}
	}
	state := &State{Format: 1, Fingerprint: c.fingerprint(), SystemID: sys.SystemID, Timeline: sys.Timeline, Database: sys.DBName, RelationID: oid, Columns: cols, Rows: map[string]Row{}}
	lsn, err := pglogrepl.ParseLSN(result.ConsistentPoint)
	if err != nil {
		return nil, err
	}
	state.LSN = uint64(lsn)
	state.Rows, err = readSnapshotRows(ctx, tx, c, cols)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return state, nil
}

func readSnapshotRows(ctx context.Context, tx pgx.Tx, c Config, cols []Column) (map[string]Row, error) {
	parts := make([]string, len(cols))
	for i, col := range cols {
		parts[i] = pgx.Identifier{col.Name}.Sanitize() + "::text"
	}
	rows, err := tx.Query(ctx, "SELECT "+strings.Join(parts, ",")+" FROM ONLY "+pgx.Identifier{c.Schema, c.Table}.Sanitize())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	data := map[string]Row{}
	bytes := 0
	for rows.Next() {
		values := make([]*string, len(cols))
		dest := make([]any, len(cols))
		for i := range dest {
			dest[i] = &values[i]
		}
		if err = rows.Scan(dest...); err != nil {
			return nil, err
		}
		row := Row{}
		for i, col := range cols {
			v := json.RawMessage("null")
			if values[i] != nil {
				v, err = decodeValue(col.OID, []byte(*values[i]))
				if err != nil {
					return nil, err
				}
			}
			row[col.Name] = v
			bytes += len(v) + len(col.Name)
		}
		id, err := rowID(row, cols)
		if err != nil {
			return nil, err
		}
		data[id] = row
		processBoundary("snapshot_row")
		if len(data) > MaxRows || bytes > MaxStateBytes/2 {
			return nil, errors.New("snapshot budget exceeded")
		}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	return data, nil
}

func validateResume(ctx context.Context, sql *pgx.Conn, repl *pgconn.PgConn, c Config, s *State) error {
	if err := validateIdentity(ctx, sql, repl, c, s); err != nil {
		return err
	}
	return validateSlot(ctx, sql, c, s, false)
}

// validateRetained accepts a dropped slot: a retained decoded commit applies
// without slot continuity, and Resume or Resnapshot still decide whether
// streaming can continue. An existing slot must satisfy the resume checks.
func validateRetained(ctx context.Context, sql *pgx.Conn, repl *pgconn.PgConn, c Config, s *State) error {
	if err := validateIdentity(ctx, sql, repl, c, s); err != nil {
		return err
	}
	return validateSlot(ctx, sql, c, s, true)
}

func validateIdentity(ctx context.Context, sql *pgx.Conn, repl *pgconn.PgConn, c Config, s *State) error {
	if s.Fingerprint != c.fingerprint() {
		return errors.New("configuration changed; explicit new binding/resync required")
	}
	sys, err := pglogrepl.IdentifySystem(ctx, repl)
	if err != nil {
		return err
	}
	if s.SystemID != sys.SystemID || s.Timeline != sys.Timeline || s.Database != sys.DBName {
		return errors.New("source identity/history changed; resync required")
	}
	oid, cols, err := preflight(ctx, sql, c)
	if err != nil {
		return err
	}
	if oid != s.RelationID || !reflect.DeepEqual(cols, s.Columns) {
		return errors.New("source schema changed; action required")
	}
	return nil
}

func validateSlot(ctx context.Context, sql *pgx.Conn, c Config, s *State, allowMissing bool) error {
	var plugin, db string
	var restart, confirmed *string
	var active bool
	err := sql.QueryRow(ctx, `SELECT plugin,database,restart_lsn::text,confirmed_flush_lsn::text,active FROM pg_replication_slots WHERE slot_name=$1`, c.Slot).Scan(&plugin, &db, &restart, &confirmed, &active)
	if allowMissing && errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil || plugin != "pgoutput" || db != s.Database || restart == nil || confirmed == nil || active {
		return errors.New("slot missing, invalid or already active; resync/ownership check required")
	}
	for _, position := range []*string{restart, confirmed} {
		lsn, err := pglogrepl.ParseLSN(*position)
		if err != nil || uint64(lsn) > s.LSN {
			return errors.New("slot advanced beyond recoverable checkpoint; resync required")
		}
	}
	return nil
}

// Cancellation can interrupt the socket write as a timeout. Preserve the local
// cancellation classification instead of reporting a source failure on pause.
func sendFeedback(ctx context.Context, repl *pgconn.PgConn, position pglogrepl.LSN) error {
	err := pglogrepl.SendStandbyStatusUpdate(ctx, repl, pglogrepl.StandbyStatusUpdate{WALWritePosition: position, WALFlushPosition: position, WALApplyPosition: position})
	if err != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}
