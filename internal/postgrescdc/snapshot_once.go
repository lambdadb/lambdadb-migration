package postgrescdc

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"
)

// SnapshotData is one complete consistent source cut, not a claim about writes
// committed after that cut. It is returned only after the read transaction commits.
type SnapshotData struct {
	RelationID uint32         `json:"relation_id"`
	Columns    []Column       `json:"columns"`
	Rows       map[string]Row `json:"rows"`
}

// ReadSnapshot needs ordinary SELECT privileges only: no publication, replication
// connection or logical slot is created. The caller persists this baseline before
// applying any target writes. Incomplete reads are safe to discard and repeat.
func ReadSnapshot(ctx context.Context, dsn, schema, table string, columns []string) (*SnapshotData, error) {
	return readSnapshotOnce(ctx, dsn, schema, table, columns, nil)
}
func readSnapshotOnce(ctx context.Context, dsn, schema, table string, columns []string, afterCut func() error) (*SnapshotData, error) {
	if !name.MatchString(schema) || !name.MatchString(table) || len(columns) > 256 {
		return nil, errors.New("invalid snapshot selection")
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.Background())
	c := Config{Schema: schema, Table: table}
	if err = lockSnapshotProjection(ctx, tx, schema, table, columns); err != nil {
		return nil, err
	}
	oid, metadata, err := inspectTable(ctx, tx, c, false)
	if err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		for _, entry := range metadata {
			columns = append(columns, entry.Name)
		}
	}
	cols, err := selectColumns(metadata, columns)
	if err != nil {
		return nil, err
	}
	if afterCut != nil {
		if err = afterCut(); err != nil {
			return nil, err
		}
	}
	rows, err := readSnapshotRows(ctx, tx, c, cols)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &SnapshotData{RelationID: oid, Columns: cols, Rows: rows}, nil
}

// A projected SELECT retains ACCESS SHARE until transaction end without requiring
// table-wide SELECT privilege for unpublished columns, as explicit LOCK TABLE does.
func lockSnapshotProjection(ctx context.Context, tx pgx.Tx, schema, table string, columns []string) error {
	projection := "*"
	if len(columns) > 0 {
		parts := make([]string, len(columns))
		for i, column := range columns {
			parts[i] = pgx.Identifier{column}.Sanitize()
		}
		projection = strings.Join(parts, ",")
	}
	rows, err := tx.Query(ctx, "SELECT "+projection+" FROM "+pgx.Identifier{schema, table}.Sanitize()+" LIMIT 0")
	if err != nil {
		return err
	}
	rows.Close()
	return rows.Err()
}
