package postgrescdc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	lambdadb "github.com/lambdadb/go-lambdadb"
)

type memoryTarget struct {
	mu           sync.Mutex
	collections  map[string]map[string]Row
	beforePut    func(string, string, Row) error
	beforeDelete func(string, string) error
}

func (m *memoryTarget) Verify(_ context.Context, gen string, expected map[string]Row) error {
	if !equalJSON(m.rows(gen), expected) {
		return errors.New("target generation differs from regional row state")
	}
	return nil
}

func (m *memoryTarget) Create(_ context.Context, _ Config) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.collections == nil {
		m.collections = map[string]map[string]Row{}
	}
	gen := fmt.Sprintf("generation_%d", len(m.collections)+1)
	m.collections[gen] = map[string]Row{}
	return gen, nil
}
func (m *memoryTarget) Put(_ context.Context, gen, id string, row Row) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.beforePut != nil {
		if err := m.beforePut(gen, id, row); err != nil {
			return err
		}
	}
	m.collections[gen][id] = row
	return nil
}
func (m *memoryTarget) Delete(_ context.Context, gen, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.beforeDelete != nil {
		if err := m.beforeDelete(gen, id); err != nil {
			return err
		}
	}
	delete(m.collections[gen], id)
	return nil
}
func (m *memoryTarget) rows(gen string) map[string]Row {
	m.mu.Lock()
	defer m.mu.Unlock()
	return cloneRows(m.collections[gen])
}

func testConfig() Config {
	return Config{BindingID: "test", Revision: 1, TargetRegion: "ap-northeast-2", AuthorityExpiresAt: time.Now().UTC().Add(time.Hour).Truncate(time.Second), SourceDSNEnv: "TEST_DSN", TargetKeyEnv: "TEST_KEY", Schema: "public", Table: "items", Publication: "test_pub", Slot: "test_slot", TargetURL: "https://test.invalid", TargetProject: "test", CollectionPrefix: "cdc_test", IndexConfigs: json.RawMessage(`{"title":{"type":"text"}}`)}
}
func openTestStore(t *testing.T, path string) *Store {
	t.Helper()
	s, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
func TestRegionalStateOwnershipAndAtomicity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint.db")
	store := openTestStore(t, path)
	if second, err := OpenStore(path); err == nil {
		second.Close()
		t.Fatal("second owner accepted")
	}
	s := decoderState()
	if err := store.Save(s); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load()
	if err != nil || !equalJSON(loaded, s) {
		t.Fatalf("roundtrip: %v", err)
	}
	bad := *s
	bad.LSN = 200
	bad.Rows = map[string]Row{"1": {"title": json.RawMessage(`"` + strings.Repeat("x", MaxStateBytes) + `"`)}}
	if err := store.Save(&bad); err == nil {
		t.Fatal("oversized state accepted")
	}
	loaded, err = store.Load()
	if err != nil || loaded.LSN != 100 {
		t.Fatal("failed save changed checkpoint")
	}
}
func TestConfigurationAuthority(t *testing.T) {
	c := testConfig()
	if err := c.Validate(); err != nil || c.WorkerRegion != c.TargetRegion {
		t.Fatal(err)
	}
	c.WorkerRegion = "us-east-1"
	if c.Validate() == nil {
		t.Fatal("cross-region placement accepted")
	}
	c = testConfig()
	c.AuthorityExpiresAt = time.Now().Add(-time.Second)
	if c.Validate() == nil {
		t.Fatal("expired authority accepted")
	}
}
func decoderState() *State {
	return &State{Format: 1, Fingerprint: "binding", SystemID: "source", RelationID: 42, LSN: 100, Columns: []Column{{Name: "id", OID: 20, Key: true, Modifier: -1}, {Name: "body", OID: 25, Modifier: -1}, {Name: "metadata", OID: 3802, Modifier: -1}}, Rows: map[string]Row{"1": {"id": json.RawMessage("1"), "body": json.RawMessage(`"retained"`), "metadata": json.RawMessage(`{"a":1,"b":2}`)}}}
}
func tuple(values ...string) *pglogrepl.TupleData {
	t := &pglogrepl.TupleData{ColumnNum: uint16(len(values))}
	for _, v := range values {
		c := &pglogrepl.TupleDataColumn{DataType: 't', Data: []byte(v)}
		if v == "<unchanged>" {
			c.DataType = 'u'
			c.Data = nil
		}
		if v == "<null>" {
			c.DataType = 'n'
			c.Data = nil
		}
		t.Columns = append(t.Columns, c)
	}
	return t
}
func TestOrderedReconstructionAndDuplicateReplay(t *testing.T) {
	d := &transactionDecoder{state: decoderState(), relationSeen: true}
	for _, m := range []pglogrepl.Message{
		&pglogrepl.BeginMessage{FinalLSN: 190},
		&pglogrepl.UpdateMessage{RelationID: 42, OldTuple: tuple("1", "<null>", "<null>"), NewTuple: tuple("9", "<unchanged>", `{}`)},
	} {
		if _, err := d.consume(m, 1); err != nil {
			t.Fatal(err)
		}
	}
	next, err := d.consume(&pglogrepl.CommitMessage{CommitLSN: 190, TransactionEndLSN: 200}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(next.Rows) != 1 || string(next.Rows["9"]["body"]) != `"retained"` || string(next.Rows["9"]["metadata"]) != `{}` || len(d.state.Rows["1"]) == 0 {
		t.Fatal("PK/TOAST/replacement or isolation failed")
	}
	d.state = next
	// Replay an old delete whose row no longer exists. Skip at Begin, before
	// trying to reconstruct it against newer state.
	for _, m := range []pglogrepl.Message{&pglogrepl.BeginMessage{FinalLSN: 90}, &pglogrepl.DeleteMessage{RelationID: 42, OldTuple: tuple("1", "<null>", "<null>")}, &pglogrepl.CommitMessage{CommitLSN: 90, TransactionEndLSN: 100}} {
		result, err := d.consume(m, 1)
		if err != nil || result != nil {
			t.Fatalf("duplicate not skipped: %v", err)
		}
	}
	if string(d.state.Rows["9"]["body"]) != `"retained"` {
		t.Fatal("duplicate regressed state")
	}
}
func TestRejectUnknownTOASTTruncateAndBudget(t *testing.T) {
	for _, tc := range []struct {
		name string
		msg  pglogrepl.Message
		size int
	}{
		{"toast", &pglogrepl.InsertMessage{RelationID: 42, Tuple: tuple("2", "<unchanged>", `{}`)}, 1},
		{"truncate", &pglogrepl.TruncateMessage{}, 1},
		{"budget", &pglogrepl.InsertMessage{RelationID: 42, Tuple: tuple("2", "text", `{}`)}, MaxTransactionBytes + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := &transactionDecoder{state: decoderState(), relationSeen: true}
			_, _ = d.consume(&pglogrepl.BeginMessage{FinalLSN: 190}, 1)
			if _, err := d.consume(tc.msg, tc.size); err == nil {
				t.Fatal("unsafe message accepted")
			}
		})
	}
}

func TestSDKWireDocumentReplacement(t *testing.T) {
	row := Row{"id": json.RawMessage("1"), "title": json.RawMessage(`"text"`), "metadata": json.RawMessage(`{"number":9007199254740993,"nested":{},"nullable":null}`)}
	wire, err := json.Marshal(lambdadb.UpsertDocsInput{Docs: []map[string]any{document("1", row)}})
	if err != nil {
		t.Fatal(err)
	}
	expected := json.RawMessage(`{"docs":[{"id":"1","title":"text","metadata":{"number":9007199254740993,"nested":{},"nullable":null}}]}`)
	if !equalJSON(json.RawMessage(wire), expected) {
		t.Fatal("SDK changed source JSON/number representation")
	}
	if equalJSON(json.RawMessage(`9007199254740993`), float64(9007199254740992)) {
		t.Fatal("visibility comparison lost integer precision")
	}
}

type fixture struct {
	dsn  string
	sql  *pgx.Conn
	c    Config
	path string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	dsn := os.Getenv("CDC_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set CDC_TEST_POSTGRES_DSN to an isolated localhost PostgreSQL")
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Host != "localhost" && cfg.Host != "127.0.0.1" {
		t.Fatal("integration source must be localhost")
	}
	ctx := context.Background()
	sql, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	c := testConfig()
	suffix := fmt.Sprintf("%x", time.Now().UnixNano())
	c.Schema = "cdc_" + suffix
	c.Publication = "cdc_pub_" + suffix
	c.Slot = "cdc_slot_" + suffix
	execSQL(t, sql, "CREATE SCHEMA "+c.Schema)
	execSQL(t, sql, "CREATE TABLE "+c.Schema+".items(id bigint PRIMARY KEY,title text,body text,metadata jsonb)")
	execSQL(t, sql, "ALTER TABLE "+c.Schema+".items ALTER COLUMN body SET STORAGE EXTERNAL")
	execSQL(t, sql, "INSERT INTO "+c.Schema+".items VALUES (1,'baseline',repeat('unchanged toast ',500),'{\"a\":1,\"b\":2}'),(2,'deleted','small',NULL)")
	execSQL(t, sql, "CREATE PUBLICATION "+c.Publication+" FOR TABLE "+c.Schema+".items")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = sql.Exec(ctx, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1 AND NOT active", c.Slot)
		_, _ = sql.Exec(ctx, "DROP PUBLICATION "+c.Publication)
		_, _ = sql.Exec(ctx, "DROP SCHEMA "+c.Schema+" CASCADE")
		_ = sql.Close(ctx)
	})
	return &fixture{dsn: dsn, sql: sql, c: c, path: filepath.Join(t.TempDir(), "checkpoint.db")}
}
func execSQL(t *testing.T, conn *pgx.Conn, query string) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), query); err != nil {
		t.Fatal(err)
	}
}

type running struct {
	cancel   context.CancelFunc
	done     chan error
	progress chan Progress
}

func start(t *testing.T, w *Worker, dsn string) *running {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	r := &running{cancel: cancel, done: make(chan error, 1), progress: make(chan Progress, 128)}
	w.Report = func(p Progress) { r.progress <- p }
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		err := w.Run(ctx, dsn)
		// pgx can report an I/O timeout when cancellation races standby feedback.
		// Match CLI shutdown only for that canceled transport result, retaining
		// unrelated schema, target and continuity failures.
		if ctx.Err() != nil && pgconn.Timeout(err) {
			err = ctx.Err()
		}
		r.done <- err
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-exited:
		case <-time.After(5 * time.Second):
			t.Error("worker cleanup timed out")
		}
	})
	return r
}
func waitStreaming(t *testing.T, r *running) Progress {
	t.Helper()
	for {
		select {
		case p := <-r.progress:
			if p.Phase == "streaming" {
				return p
			}
		case err := <-r.done:
			t.Fatalf("worker stopped: %v", err)
		case <-time.After(2 * time.Minute):
			t.Fatal("streaming timeout")
		}
	}
}
func stop(t *testing.T, r *running) {
	t.Helper()
	select {
	case err := <-r.done:
		t.Fatalf("worker exited before requested shutdown: %v", err)
	default:
	}
	r.cancel()
	select {
	case err := <-r.done:
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("stop: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop")
	}
}
func waitRows(t *testing.T, r *running, store *Store, predicate func(*State) bool) *State {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.After(2 * time.Minute)
	for {
		select {
		case <-ticker.C:
			s, err := store.Load()
			if err != nil {
				t.Fatal(err)
			}
			if s != nil && predicate(s) {
				return s
			}
		case err := <-r.done:
			t.Fatalf("worker stopped: %v", err)
		case <-timeout:
			t.Fatal("checkpoint timeout")
		}
	}
}
func sourceRows(t *testing.T, f *fixture) map[string]Row {
	t.Helper()
	rows, err := f.sql.Query(context.Background(), "SELECT id::text,title,body,metadata::text FROM "+f.c.Schema+".items")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	result := map[string]Row{}
	for rows.Next() {
		var id string
		var title, body, metadata *string
		if err = rows.Scan(&id, &title, &body, &metadata); err != nil {
			t.Fatal(err)
		}
		row := Row{"id": json.RawMessage(id)}
		for k, v := range map[string]*string{"title": title, "body": body, "metadata": metadata} {
			if v == nil {
				row[k] = json.RawMessage("null")
			} else if k == "metadata" {
				row[k] = json.RawMessage(*v)
			} else {
				row[k], _ = json.Marshal(*v)
			}
		}
		result[id] = row
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestPostgresSnapshotHandoffAndRecovery(t *testing.T) {
	f := newFixture(t)
	store := openTestStore(t, f.path)
	target := &memoryTarget{}
	baseline := map[string]Row{}
	target.beforePut = func(gen, id string, row Row) error {
		if gen == "generation_1" && baseline[id] == nil {
			baseline[id] = row
		}
		return nil
	}
	// This transaction starts before the snapshot and writes/commits afterward.
	spanning, err := pgx.Connect(context.Background(), f.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer spanning.Close(context.Background())
	execSQL(t, spanning, "BEGIN")
	execSQL(t, spanning, "SELECT 1")
	w := &Worker{Config: f.c, Store: store, Target: target, afterSnapshotImport: func() error {
		_, err := spanning.Exec(context.Background(), "UPDATE "+f.c.Schema+".items SET title='after boundary',metadata='{\"a\":3}' WHERE id=1; DELETE FROM "+f.c.Schema+".items WHERE id=2; INSERT INTO "+f.c.Schema+".items VALUES(3,'insert','new','{}'); COMMIT")
		if err != nil {
			return err
		}
		_, err = f.sql.Exec(context.Background(), "BEGIN; INSERT INTO "+f.c.Schema+".items VALUES(4,'aborted','bad','{}'); ROLLBACK")
		return err
	}}
	r := start(t, w, f.dsn)
	p := waitStreaming(t, r)
	s := waitRows(t, r, store, func(s *State) bool { return len(s.Rows) == 2 && s.Rows["3"] != nil })
	if !equalJSON(s.Rows, sourceRows(t, f)) || !equalJSON(target.rows(p.Generation), s.Rows) {
		t.Fatal("snapshot/WAL handoff mismatch")
	}
	stop(t, r)
	if len(baseline) != 3 || string(baseline["1"]["title"]) != `"baseline"` || baseline["2"] == nil || baseline["4"] != nil {
		t.Fatal("exported baseline mixed concurrent source state")
	}
	// Changes while the worker and central management plane are unavailable.
	execSQL(t, f.sql, "UPDATE "+f.c.Schema+".items SET id=10,metadata='{}' WHERE id=1; DELETE FROM "+f.c.Schema+".items WHERE id=3; INSERT INTO "+f.c.Schema+".items VALUES(3,'reinsert','newer','null')")
	w = &Worker{Config: f.c, Store: store, Target: target}
	r = start(t, w, f.dsn)
	p2 := waitStreaming(t, r)
	s = waitRows(t, r, store, func(s *State) bool { return s.Rows["10"] != nil && string(s.Rows["3"]["title"]) == `"reinsert"` })
	if p.Generation == p2.Generation || !equalJSON(s.Rows, sourceRows(t, f)) || !equalJSON(target.rows(p2.Generation), s.Rows) {
		t.Fatal("restart/PK/TOAST/delete-reinsert mismatch")
	}
	// Simulate a delayed stale request. It can only affect its retired generation.
	_ = target.Delete(context.Background(), p.Generation, "10")
	if target.rows(p2.Generation)["10"] == nil {
		t.Fatal("stale generation regressed successor")
	}
	stop(t, r)
}

func TestPostgresSnapshotReaderCrash(t *testing.T) {
	f := newFixture(t)
	store := openTestStore(t, f.path)
	target := &memoryTarget{}
	boom := errors.New("snapshot reader lost")
	r := start(t, &Worker{Config: f.c, Store: store, Target: target, afterSnapshotImport: func() error { return boom }}, f.dsn)
	select {
	case err := <-r.done:
		if !errors.Is(err, boom) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot fault timeout")
	}
	r.cancel()
	s, err := store.Load()
	if err != nil || s != nil || len(target.collections) != 0 {
		t.Fatal("incomplete snapshot became recoverable/applied")
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openTestStore(t, f.path)
	execSQL(t, f.sql, "DELETE FROM "+f.c.Schema+".items WHERE id=2; UPDATE "+f.c.Schema+".items SET title='fresh baseline' WHERE id=1")
	r = start(t, &Worker{Config: f.c, Store: store, Target: target}, f.dsn)
	p := waitStreaming(t, r)
	s, err = store.Load()
	if err != nil || !equalJSON(s.Rows, sourceRows(t, f)) || !equalJSON(target.rows(p.Generation), s.Rows) {
		t.Fatal("lost snapshot was not rebuilt consistently")
	}
	stop(t, r)
}

func TestPostgresRejectLostContinuityAndConfigDrift(t *testing.T) {
	for _, cause := range []string{"slot", "schema", "config", "publication"} {
		t.Run(cause, func(t *testing.T) {
			f := newFixture(t)
			store := openTestStore(t, f.path)
			target := &memoryTarget{}
			r := start(t, &Worker{Config: f.c, Store: store, Target: target}, f.dsn)
			_ = waitStreaming(t, r)
			stop(t, r)
			before, err := store.Load()
			if err != nil {
				t.Fatal(err)
			}
			switch cause {
			case "slot":
				execSQL(t, f.sql, "SELECT pg_drop_replication_slot('"+f.c.Slot+"')")
			case "schema":
				execSQL(t, f.sql, "ALTER TABLE "+f.c.Schema+".items ADD COLUMN unexpected text")
			case "config":
				f.c.Revision++
			case "publication":
				execSQL(t, f.sql, "ALTER PUBLICATION "+f.c.Publication+" SET (publish='insert,update')")
			}
			r = start(t, &Worker{Config: f.c, Store: store, Target: target}, f.dsn)
			select {
			case err = <-r.done:
				if err == nil {
					t.Fatal("unsafe resume accepted")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("resume rejection timeout")
			}
			r.cancel()
			after, err := store.Load()
			if err != nil || !equalJSON(before, after) || len(target.collections) != 1 {
				t.Fatal("rejected resume changed target/checkpoint")
			}
		})
	}
}

func TestPostgresCrashBoundaries(t *testing.T) {
	for _, boundary := range []string{"after_apply", "after_save", "partial_pk_move"} {
		t.Run(boundary, func(t *testing.T) {
			f := newFixture(t)
			target := &memoryTarget{}
			store := openTestStore(t, f.path)
			r := start(t, &Worker{Config: f.c, Store: store, Target: target}, f.dsn)
			_ = waitStreaming(t, r)
			stop(t, r)
			boom := errors.New("simulated crash")
			w := &Worker{Config: f.c, Store: store, Target: target}
			if boundary == "after_apply" {
				w.afterApply = func() error { return boom }
			}
			if boundary == "after_save" {
				w.afterSave = func() error { return boom }
			}
			if boundary == "partial_pk_move" {
				target.beforeDelete = func(string, string) error { return boom }
			}
			r = start(t, w, f.dsn)
			p := waitStreaming(t, r)
			execSQL(t, f.sql, "UPDATE "+f.c.Schema+".items SET id=9,title='moved',metadata='{}' WHERE id=1")
			select {
			case err := <-r.done:
				if !errors.Is(err, boom) {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("fault did not fire")
			}
			r.cancel()
			before, err := store.Load()
			if err != nil {
				t.Fatal(err)
			}
			if boundary != "after_save" && before.Rows["1"] == nil {
				t.Fatal("checkpoint advanced before successful apply")
			}
			// Close/reopen the actual durable store, rather than reusing RAM state.
			if err = store.Close(); err != nil {
				t.Fatal(err)
			}
			store = openTestStore(t, f.path)
			target.beforeDelete = nil
			r = start(t, &Worker{Config: f.c, Store: store, Target: target}, f.dsn)
			recovered := waitStreaming(t, r)
			s := waitRows(t, r, store, func(s *State) bool { return s.Rows["9"] != nil })
			if recovered.Generation == p.Generation || !equalJSON(s.Rows, sourceRows(t, f)) || !equalJSON(target.rows(recovered.Generation), s.Rows) {
				t.Fatal("crash recovery mismatch")
			}
			stop(t, r)
		})
	}
}
