package postgrescdc

import (
	"context"
	"encoding/json"
	"testing"
)

func TestSnapshotSelectionMatchesLogicalStream(t *testing.T) {
	for _, variant := range []string{"boolean", "inheritance", "included-key-attribute"} {
		t.Run(variant, func(t *testing.T) {
			f, c := captureFixture(t)
			table := f.c.Schema + ".items"
			switch variant {
			case "boolean":
				execSQL(t, f.sql, "ALTER TABLE "+table+" ADD COLUMN enabled boolean; UPDATE "+table+" SET enabled=(id=1)")
				execSQL(t, f.sql, "INSERT INTO "+table+"(id,title,body) VALUES(3,'null flag','body')")
				execSQL(t, f.sql, "ALTER PUBLICATION "+f.c.Publication+" SET TABLE "+table+" (id,title,body,enabled)")
				c.Selection.Columns = append(c.Selection.Columns, "enabled")
			case "inheritance":
				execSQL(t, f.sql, "CREATE TABLE "+f.c.Schema+".child() INHERITS ("+table+")")
				execSQL(t, f.sql, "INSERT INTO "+f.c.Schema+".child(id,title,body) VALUES(99,'unpublished','body')")
				execSQL(t, f.sql, "ALTER PUBLICATION "+f.c.Publication+" SET TABLE ONLY "+table+" (id,title,body)")
			case "included-key-attribute":
				execSQL(t, f.sql, "ALTER TABLE "+table+" DROP CONSTRAINT items_pkey, ADD PRIMARY KEY (id) INCLUDE (title)")
			}
			check := func(rows map[string]Row) {
				t.Helper()
				count := 2
				if variant == "boolean" {
					count = 3
					for id, want := range map[string]string{"1": "true", "2": "false", "3": "null"} {
						if string(rows[id]["enabled"]) != want {
							t.Fatalf("row %s flag=%s, want %s", id, rows[id]["enabled"], want)
						}
					}
				}
				if len(rows) != count || rows["99"] != nil {
					t.Fatalf("unexpected snapshot rows: %v", rows)
				}
			}
			snapshot, err := ReadSnapshot(context.Background(), f.dsn, f.c.Schema, "items", c.Selection.Columns)
			if err != nil {
				t.Fatal(err)
			}
			check(snapshot.Rows)
			state, err := c.Initialize(context.Background(), f.dsn)
			if err != nil {
				t.Fatal(err)
			}
			check(state.Rows)
			fresh, err := c.Resnapshot(context.Background(), f.dsn)
			if err != nil {
				t.Fatal(err)
			}
			check(fresh.Rows)
			if err = c.Store.Save(fresh); err != nil {
				t.Fatal(err)
			}
			cancel, done := runCapture(t, c, f.dsn, func(context.Context, *State, *State) error { return nil })
			execSQL(t, f.sql, "UPDATE ONLY "+table+" SET title='changed' WHERE id=1")
			awaitCapture(t, c, func(s *State) bool { return string(s.Rows["1"]["title"]) == `"changed"` })
			stopCapture(t, cancel, done)
		})
	}
}

func TestBooleanWireRepresentations(t *testing.T) {
	for input, want := range map[string]string{"t": "true", "true": "true", "f": "false", "false": "false"} {
		got, err := decodeValue(16, []byte(input))
		if err != nil || string(got) != want {
			t.Fatalf("%s: %s %v", input, got, err)
		}
	}
	if _, err := decodeValue(16, []byte("invalid")); err == nil {
		t.Fatal("invalid boolean accepted")
	}
}

func TestRowComparisonPreservesNumericAndStructuralMeaning(t *testing.T) {
	row := func(raw string) Row { return Row{"value": json.RawMessage(raw)} }
	for _, test := range []struct {
		left, right string
		equal       bool
	}{
		{`1`, `1.0`, true}, {`1e3`, `1000`, true},
		{`9007199254740992`, `9007199254740993`, false},
		{`{"a":1,"b":2}`, `{"b":2.0,"a":1}`, true},
		{`[1,2]`, `[2,1]`, false}, {`null`, `"null"`, false},
	} {
		if EqualRows(row(test.left), row(test.right)) != test.equal {
			t.Fatalf("comparison: %+v", test)
		}
	}
}
