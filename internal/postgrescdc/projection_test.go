package postgrescdc

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5/pgconn"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func projectedFixture(t *testing.T) *fixture {
	t.Helper()
	f := newFixture(t)
	execSQL(t, f.sql, "ALTER TABLE "+f.c.Schema+".items ADD COLUMN private_amount numeric, ADD COLUMN private_derived text GENERATED ALWAYS AS (title || '-private') STORED")
	execSQL(t, f.sql, "UPDATE "+f.c.Schema+".items SET private_amount=123456789.123456789")
	execSQL(t, f.sql, "ALTER PUBLICATION "+f.c.Publication+" SET TABLE "+f.c.Schema+".items (body,title,id)")
	return f
}
func projectedExpected(id, title, body string) Row {
	titleJSON, _ := json.Marshal(title)
	bodyJSON, _ := json.Marshal(body)
	return Row{"id": json.RawMessage(id), "title": titleJSON, "body": bodyJSON}
}
func TestPostgresSelectedColumnsSnapshotStreamAndRestart(t *testing.T) {
	f := projectedFixture(t)
	store := openTestStore(t, f.path)
	target := &memoryTarget{}
	var snapshot map[string]Row
	w := &Worker{Config: f.c, Store: store, Target: target, afterSnapshotImport: func() error {
		_, err := f.sql.Exec(context.Background(), "UPDATE "+f.c.Schema+".items SET title='after boundary',metadata='{\"private\":true}',private_amount=987 WHERE id=1; DELETE FROM "+f.c.Schema+".items WHERE id=2; INSERT INTO "+f.c.Schema+".items(id,title,body,metadata) VALUES(3,'insert','new','{\"excluded\":true}')")
		return err
	}}
	target.beforePut = func(gen, id string, row Row) error {
		if gen == "generation_1" {
			if snapshot == nil {
				snapshot = map[string]Row{}
			}
			if snapshot[id] == nil {
				snapshot[id] = row
			}
		}
		return nil
	}
	r := start(t, w, f.dsn)
	p := waitStreaming(t, r)
	state := waitRows(t, r, store, func(s *State) bool {
		return len(s.Rows) == 2 && s.Rows["3"] != nil && string(s.Rows["1"]["title"]) == `"after boundary"`
	})
	stop(t, r)
	want := map[string]Row{"1": projectedExpected("1", "after boundary", strings.Repeat("unchanged toast ", 500)), "3": projectedExpected("3", "insert", "new")}
	if !equalJSON(state.Rows, want) || !equalJSON(target.rows(p.Generation), want) {
		t.Fatal("selected snapshot/stream differs from independently expected complete row set")
	}
	if string(snapshot["1"]["title"]) != `"baseline"` || snapshot["2"] == nil {
		t.Fatal("selected exported snapshot mixed later source commits")
	}
	names := []string{}
	for _, col := range state.Columns {
		names = append(names, col.Name)
	}
	if !reflect.DeepEqual(names, []string{"id", "title", "body"}) {
		t.Fatal("publication declaration order changed tuple projection", names)
	}
	raw, _ := json.Marshal(state)
	for _, excluded := range []string{"metadata", "private_amount", "private_derived", "123456789"} {
		if strings.Contains(string(raw), excluded) {
			t.Fatal("excluded column/value reached regional state", excluded)
		}
	}
	// Excluded schema and values are outside the selected mapping. PK movement
	// still needs the selected unchanged TOAST value from the regional row mirror.
	execSQL(t, f.sql, "ALTER TABLE "+f.c.Schema+".items ADD COLUMN another_private timestamp; UPDATE "+f.c.Schema+".items SET id=10,title='moved',private_amount=111 WHERE id=1; DELETE FROM "+f.c.Schema+".items WHERE id=3; INSERT INTO "+f.c.Schema+".items(id,title,body) VALUES(3,'reinsert','newer')")
	r = start(t, &Worker{Config: f.c, Store: store, Target: target}, f.dsn)
	p = waitStreaming(t, r)
	state = waitRows(t, r, store, func(s *State) bool { return s.Rows["10"] != nil && string(s.Rows["3"]["title"]) == `"reinsert"` })
	stop(t, r)
	want = map[string]Row{"10": projectedExpected("10", "moved", strings.Repeat("unchanged toast ", 500)), "3": projectedExpected("3", "reinsert", "newer")}
	if !equalJSON(state.Rows, want) || !equalJSON(target.rows(p.Generation), want) {
		t.Fatal("selected restart/PK/TOAST/delete-reinsert mismatch")
	}
}
func TestPostgresSelectedColumnsPreflightAndDrift(t *testing.T) {
	for _, scenario := range []string{"row-filter", "selected-unsupported", "selected-generated", "selection-changed", "excluded-id"} {
		t.Run(scenario, func(t *testing.T) {
			f := projectedFixture(t)
			switch scenario {
			case "row-filter":
				execSQL(t, f.sql, "ALTER PUBLICATION "+f.c.Publication+" SET TABLE "+f.c.Schema+".items(id,title,body) WHERE (id>0)")
			case "selected-unsupported":
				execSQL(t, f.sql, "ALTER PUBLICATION "+f.c.Publication+" SET TABLE "+f.c.Schema+".items(id,title,private_amount)")
			case "selected-generated":
				_, err := f.sql.Exec(context.Background(), "ALTER PUBLICATION "+f.c.Publication+" SET TABLE "+f.c.Schema+".items(id,title,private_derived)")
				if err != nil {
					var pgError *pgconn.PgError
					if errors.As(err, &pgError) && pgError.Code == "42P10" {
						t.Log("server rejects generated column publication before consumer admission")
						return
					}
					t.Fatal(err)
				}
			case "excluded-id":
				execSQL(t, f.sql, "ALTER TABLE "+f.c.Schema+".items RENAME COLUMN id TO product_id; ALTER TABLE "+f.c.Schema+".items ADD COLUMN id text; ALTER PUBLICATION "+f.c.Publication+" SET TABLE "+f.c.Schema+".items(product_id,title,body)")
			}
			oid, columns, err := preflight(context.Background(), f.sql, f.c)
			if scenario == "excluded-id" {
				if err != nil || len(columns) != 3 || columns[0].Name != "product_id" {
					t.Fatal("unpublished non-key id was treated as a target field", err)
				}
				return
			}
			if scenario != "selection-changed" {
				if err == nil {
					t.Fatal("unsupported selected data accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			state := &State{Fingerprint: f.c.fingerprint(), RelationID: oid, Columns: columns}
			execSQL(t, f.sql, "ALTER PUBLICATION "+f.c.Publication+" SET TABLE "+f.c.Schema+".items(id,title)")
			changedOID, changed, err := preflight(context.Background(), f.sql, f.c)
			if err != nil {
				t.Fatal(err)
			}
			if changedOID != state.RelationID || reflect.DeepEqual(changed, state.Columns) {
				t.Fatal("changed selected schema was not distinguishable for resume/drift rejection")
			}
		})
	}
}
func TestPostgresSelectedColumnsRespectSQLPrivileges(t *testing.T) {
	f := projectedFixture(t)
	ctx := context.Background()
	role := "projection_" + strings.TrimPrefix(f.c.Schema, "cdc_")
	execSQL(t, f.sql, "CREATE ROLE "+role)
	t.Cleanup(func() { execSQL(t, f.sql, "DROP OWNED BY "+role); execSQL(t, f.sql, "DROP ROLE "+role) })
	execSQL(t, f.sql, "GRANT USAGE ON SCHEMA "+f.c.Schema+" TO "+role)
	execSQL(t, f.sql, "GRANT SELECT(id,title,body) ON "+f.c.Schema+".items TO "+role)
	reader, err := pgx.Connect(ctx, f.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close(ctx)
	execSQL(t, reader, "SET ROLE "+role)
	_, cols, err := preflight(ctx, reader, f.c)
	if err != nil {
		t.Fatal(err)
	}
	parts := []string{}
	for _, col := range cols {
		parts = append(parts, pgx.Identifier{col.Name}.Sanitize()+"::text")
	}
	rows, err := reader.Query(ctx, "SELECT "+strings.Join(parts, ",")+" FROM "+pgx.Identifier{f.c.Schema, f.c.Table}.Sanitize())
	if err != nil {
		t.Fatal("projection requires access to an unpublished value", err)
	}
	rows.Close()
	if _, err = reader.Exec(ctx, "SELECT private_amount FROM "+f.c.Schema+".items"); err == nil {
		t.Fatal("SQL column privilege fixture was ineffective")
	}
}
