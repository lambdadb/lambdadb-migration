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
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}
type tableColumn struct {
	Column
	Generated string
}

func inspectTable(ctx context.Context, conn sourceQuery, c Config, continuous bool) (uint32, []tableColumn, error) {
	var oid uint32
	var kind, identity string
	var rls, partition bool
	var version int
	if err := conn.QueryRow(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&version); err != nil {
		return 0, nil, err
	}
	if version < 150000 {
		return 0, nil, errors.New("PostgreSQL 15+ required")
	}
	err := conn.QueryRow(ctx, `SELECT c.oid,c.relkind::text,c.relreplident::text,c.relrowsecurity,c.relispartition
	 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2`, c.Schema, c.Table).Scan(&oid, &kind, &identity, &rls, &partition)
	if err != nil {
		return 0, nil, err
	}
	if kind != "r" || (continuous && identity != "d") || rls || partition {
		return 0, nil, errors.New("ordinary non-RLS, non-partitioned table with default replica identity required")
	}
	rows, err := conn.Query(ctx, `SELECT a.attname,a.atttypid,a.atttypmod,
	 COALESCE(a.attnum=ANY(i.indkey),false),a.attgenerated::text
	 FROM pg_attribute a LEFT JOIN pg_index i ON i.indrelid=a.attrelid AND i.indisprimary
	 WHERE a.attrelid=$1 AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum`, oid)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()
	var tableColumns []tableColumn
	keys := 0
	for rows.Next() {
		var col Column
		var generated string
		if err := rows.Scan(&col.Name, &col.OID, &col.Modifier, &col.Key, &generated); err != nil {
			return 0, nil, err
		}
		if col.Key {
			keys++
		}
		tableColumns = append(tableColumns, tableColumn{col, generated})
	}
	if err := rows.Err(); err != nil {
		return 0, nil, err
	}
	rows.Close()
	if keys != 1 {
		return 0, nil, errors.New("single-column primary key required")
	}
	return oid, tableColumns, nil
}

func preflight(ctx context.Context, conn *pgx.Conn, c Config) (uint32, []Column, error) {
	oid, tableColumns, err := inspectTable(ctx, conn, c, true)
	if err != nil {
		return 0, nil, err
	}
	var all, ins, upd, del, trunc bool
	if err := conn.QueryRow(ctx, `SELECT puballtables,pubinsert,pubupdate,pubdelete,pubtruncate FROM pg_publication WHERE pubname=$1`, c.Publication).Scan(&all, &ins, &upd, &del, &trunc); err != nil {
		return 0, nil, err
	}
	if all || !ins || !upd || !del || !trunc {
		return 0, nil, errors.New("dedicated publication with insert/update/delete/truncate required")
	}
	pubs, err := conn.Query(ctx, `SELECT schemaname,tablename,attnames,rowfilter FROM pg_publication_tables WHERE pubname=$1`, c.Publication)
	if err != nil {
		return 0, nil, err
	}
	defer pubs.Close()
	count := 0
	var published []string
	for pubs.Next() {
		var schema, table string
		var names []string
		var filter *string
		if err := pubs.Scan(&schema, &table, &names, &filter); err != nil {
			return 0, nil, err
		}
		if schema != c.Schema || table != c.Table || filter != nil || len(names) == 0 {
			return 0, nil, errors.New("publication selection mismatch")
		}
		published = names
		count++
	}
	if err := pubs.Err(); err != nil {
		return 0, nil, err
	}
	if count != 1 {
		return 0, nil, errors.New("publication must select exactly one table")
	}
	cols, err := selectColumns(tableColumns, published)
	return oid, cols, err
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
		if string(value) == "t" {
			return json.RawMessage("true"), nil
		}
		if string(value) == "f" {
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
	rows, err := tx.Query(ctx, "SELECT "+strings.Join(parts, ",")+" FROM "+pgx.Identifier{c.Schema, c.Table}.Sanitize())
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
	var plugin, db string
	var restart, confirmed *string
	var active bool
	err = sql.QueryRow(ctx, `SELECT plugin,database,restart_lsn::text,confirmed_flush_lsn::text,active FROM pg_replication_slots WHERE slot_name=$1`, c.Slot).Scan(&plugin, &db, &restart, &confirmed, &active)
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
