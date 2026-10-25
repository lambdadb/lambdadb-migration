package postgrescdc

import (
	"context"
	"encoding/json"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestSQLOnlySnapshotConsistentCutAndColumnPrivileges(t *testing.T) {
	f := projectedFixture(t)
	ctx := context.Background()
	execSQL(t, f.sql, "DROP PUBLICATION "+f.c.Publication)
	execSQL(t, f.sql, "ALTER TABLE "+f.c.Schema+".items REPLICA IDENTITY FULL")
	role := f.c.Schema + "_snapshot"
	execSQL(t, f.sql, "CREATE ROLE "+role+" LOGIN PASSWORD 'snapshot-local-only' NOREPLICATION")
	t.Cleanup(func() { execSQL(t, f.sql, "DROP OWNED BY "+role); execSQL(t, f.sql, "DROP ROLE "+role) })
	execSQL(t, f.sql, "GRANT USAGE ON SCHEMA "+f.c.Schema+" TO "+role)
	execSQL(t, f.sql, "GRANT SELECT(id,title,body) ON "+f.c.Schema+".items TO "+role)
	parsed, err := url.Parse(f.dsn)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(role, "snapshot-local-only")
	result, err := readSnapshotOnce(ctx, parsed.String(), f.c.Schema, "items", []string{"body", "title", "id"}, func() error {
		_, e := f.sql.Exec(ctx, "UPDATE "+f.c.Schema+".items SET title='after cut' WHERE id=1;DELETE FROM "+f.c.Schema+".items WHERE id=2;INSERT INTO "+f.c.Schema+".items(id,title,body) VALUES(3,'later','new')")
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]Row{"1": projectedExpected("1", "baseline", strings.Repeat("unchanged toast ", 500)), "2": projectedExpected("2", "deleted", "small")}
	if !equalJSON(result.Rows, want) {
		t.Fatalf("snapshot mixed source cuts: %v", result.Rows)
	}
	names := []string{}
	for _, c := range result.Columns {
		names = append(names, c.Name)
	}
	if !reflect.DeepEqual(names, []string{"id", "title", "body"}) {
		t.Fatal("projection order mismatch", names)
	}
	raw, _ := json.Marshal(result)
	if strings.Contains(string(raw), "metadata") || strings.Contains(string(raw), "private_amount") {
		t.Fatal("excluded data entered snapshot")
	}
	next, err := ReadSnapshot(ctx, parsed.String(), f.c.Schema, "items", []string{"id", "title", "body"})
	if err != nil {
		t.Fatal(err)
	}
	if len(next.Rows) != 2 || next.Rows["3"] == nil || string(next.Rows["1"]["title"]) != `"after cut"` {
		t.Fatal("new snapshot did not establish a new cut")
	}
	var slots int
	if err = f.sql.QueryRow(ctx, "SELECT count(*) FROM pg_replication_slots WHERE slot_name=$1", f.c.Slot).Scan(&slots); err != nil || slots != 0 {
		t.Fatal("snapshot created a replication slot")
	}
	reader, err := pgx.Connect(ctx, parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close(ctx)
	if _, err = reader.Exec(ctx, "SELECT private_amount FROM "+f.c.Schema+".items"); err == nil {
		t.Fatal("column privilege fixture ineffective")
	}
}
func TestSQLSnapshotRejectsIncompleteOrUnsafeSelection(t *testing.T) {
	f := projectedFixture(t)
	for _, columns := range [][]string{{"title"}, {"id", "id"}, {"id", "missing"}, {"id", "private_amount"}, {"id", "private_derived"}} {
		if _, err := ReadSnapshot(context.Background(), f.dsn, f.c.Schema, "items", columns); err == nil {
			t.Fatalf("unsafe projection accepted: %v", columns)
		}
	}
	execSQL(t, f.sql, "ALTER TABLE "+f.c.Schema+".items ENABLE ROW LEVEL SECURITY")
	if _, err := ReadSnapshot(context.Background(), f.dsn, f.c.Schema, "items", []string{"id"}); err == nil {
		t.Fatal("RLS source accepted")
	}
}
