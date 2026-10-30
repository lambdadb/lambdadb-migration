package statefile

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"path"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"

	bolt "go.etcd.io/bbolt"
)

var ErrNotFound = errors.New("state object not found")
var ErrConflict = errors.New("state object condition failed")

// Objects must implement absent-only creation for an empty ETag, and exact CAS
// otherwise. Unknown outcomes must be errors, never success or absence.
type Objects interface {
	Get(context.Context, string, int64) ([]byte, string, error)
	Put(context.Context, string, []byte, string) (string, error)
}

type StoreIdentity struct {
	Format int    `json:"format"`
	ID     string `json:"id"`
	Region string `json:"region"`
}

type Binding struct {
	StoreID      string `json:"store_id"`
	Region       string `json:"region"`
	ConnectionID string `json:"connection_id"`
	Fingerprint  string `json:"fingerprint"`
}

type reference struct {
	Key    string `json:"key"`
	Digest string `json:"digest"`
}

type head struct {
	Format             int       `json:"format"`
	Binding            Binding   `json:"binding"`
	Epoch              uint64    `json:"epoch"`
	Owner              string    `json:"owner"`
	Revision           uint64    `json:"revision"`
	StateRevision      uint64    `json:"state_revision"`
	CheckpointRevision uint64    `json:"checkpoint_revision"`
	Checkpoint         reference `json:"checkpoint"`
	Tail               reference `json:"tail"`
	Depth              int       `json:"depth"`
	ReplayBytes        uint64    `json:"replay_bytes"`
	Bytes              uint64    `json:"bytes"`
}

type checkpoint struct {
	Format   int              `json:"format"`
	Binding  Binding          `json:"binding"`
	Revision uint64           `json:"revision"`
	Files    map[string]Image `json:"files"`
}

type record struct {
	Format   int       `json:"format"`
	Binding  Binding   `json:"binding"`
	Revision uint64    `json:"revision"`
	Previous reference `json:"previous"`
	File     string    `json:"file"`
	Changes  []change  `json:"changes"`
}

const maxObjectBytes = 192 << 20
const maxReplayRecords = 128
const maxRetainedBytes = 1 << 30

var hexID = regexp.MustCompile(`^[a-f0-9]{32}$`)
var hashID = regexp.MustCompile(`^[a-f0-9]{64}$`)
var connectionID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// Replica is one process's conditional-write authority. It never reloads a newer
// ETag on commit failure; another owner or an unknown response stops both handles.
type Replica struct {
	mu      sync.Mutex
	failed  atomic.Bool
	objects Objects
	ctx     context.Context
	prefix  string
	root    string
	head    head
	etag    string
	files   map[string]Image
}

func Token() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", errors.New("state identity unavailable")
	}
	return hex.EncodeToString(raw[:]), nil
}

func ValidatePrefix(prefix string) bool {
	return len(prefix) > 1 && len(prefix) <= 256 && strings.HasSuffix(prefix, "/") && path.Clean(prefix) == strings.TrimSuffix(prefix, "/") && !strings.HasPrefix(prefix, "/") && !strings.Contains(prefix, "..")
}

func OpenReplica(ctx context.Context, objects Objects, prefix string, binding Binding, epoch uint64) (*Replica, error) {
	if objects == nil || !ValidatePrefix(prefix) || !hexID.MatchString(binding.StoreID) || binding.Region == "" || !connectionID.MatchString(binding.ConnectionID) || !hashID.MatchString(binding.Fingerprint) || epoch == 0 || epoch > math.MaxInt64 {
		return nil, errors.New("invalid remote state configuration")
	}
	raw, _, err := objects.Get(ctx, prefix+"store.json", 4096)
	var marker StoreIdentity
	if err != nil || json.Unmarshal(raw, &marker) != nil || marker.Format != 1 || marker.ID != binding.StoreID || marker.Region != binding.Region {
		return nil, errors.New("remote state store unavailable or identity mismatch")
	}
	owner, err := Token()
	if err != nil {
		return nil, err
	}
	return &Replica{objects: objects, ctx: context.WithoutCancel(ctx), prefix: prefix,
		root: prefix + "workers/" + binding.ConnectionID + "/", head: head{Format: 1, Binding: binding, Epoch: epoch, Owner: owner}}, nil
}

// Initialize publishes a coherent initial pair before any worker observation or
// external mutation. It never adopts an existing head, including an empty one.
func (r *Replica) Initialize(files map[string]Image) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.etag != "" || validateFiles(files) != nil {
		return errors.New("invalid initial recovery state")
	}
	r.files = files
	return r.publishCheckpoint()
}

// Recover verifies the current remote state, then conditionally claims its head.
// The caller must have established stopped-predecessor ownership. A lost response
// or conflicting claim is never retried with newly discovered authority.
func (r *Replica) Recover() (map[string]Image, error) {
	return r.load(true)
}

// Inspect validates recovery state without claiming ownership or modifying S3.
func (r *Replica) Inspect() (map[string]Image, error) {
	return r.load(false)
}

func (r *Replica) load(claim bool) (map[string]Image, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	raw, etag, err := r.objects.Get(r.ctx, r.root+"head.json", 65536)
	var h head
	if err != nil || etag == "" || json.Unmarshal(raw, &h) != nil || h.Format != 1 || h.Binding != r.head.Binding || (claim && h.Epoch > r.head.Epoch) || h.Epoch == 0 || h.Epoch > math.MaxInt64 || !hexID.MatchString(h.Owner) || h.Revision == 0 || h.Revision >= math.MaxInt64 || h.StateRevision > math.MaxInt64 || h.CheckpointRevision > h.StateRevision || h.Depth < 0 || h.Depth > maxReplayRecords || h.Bytes > maxRetainedBytes || h.ReplayBytes > MaxRecoveryBytes {
		return nil, errors.New("remote recovery head invalid or unavailable")
	}
	var cp checkpoint
	if err := r.read(h.Checkpoint, "checkpoints", &cp); err != nil {
		return nil, err
	}
	if cp.Format != 1 || cp.Binding != h.Binding || cp.Revision != h.CheckpointRevision || validateFiles(cp.Files) != nil {
		return nil, errors.New("invalid remote recovery checkpoint")
	}
	var records []record
	var replayBytes int
	next, revision := h.Tail, h.StateRevision
	for i := 0; i < h.Depth; i++ {
		var entry record
		if err := r.read(next, "records", &entry); err != nil {
			return nil, err
		}
		if revision == 0 || entry.Format != 1 || entry.Binding != h.Binding || entry.Revision != revision || (entry.File != "connection" && entry.File != "source") {
			return nil, errors.New("invalid recovery record sequence")
		}
		if len(entry.Changes) > 40000 {
			return nil, errors.New("recovery change budget exceeded")
		}
		for _, change := range entry.Changes {
			replayBytes += len(change.Data)
			if replayBytes > MaxRecoveryBytes {
				return nil, errors.New("recovery replay byte budget exceeded")
			}
		}
		records = append(records, entry)
		next, revision = entry.Previous, revision-1
	}
	if next != (reference{}) || revision != cp.Revision || uint64(replayBytes) != h.ReplayBytes {
		return nil, errors.New("incomplete recovery record chain")
	}
	for i := len(records) - 1; i >= 0; i-- {
		entry := records[i]
		updated, err := applyChanges(cp.Files[entry.File], entry.Changes)
		if err != nil {
			return nil, err
		}
		cp.Files[entry.File] = updated
		if err := validateFiles(cp.Files); err != nil {
			return nil, err
		}
	}
	if claim {
		h.Epoch, h.Owner, h.Revision = r.head.Epoch, r.head.Owner, h.Revision+1
		if err := r.putHead(h, etag); err != nil {
			return nil, err
		}
	}
	r.files = cp.Files
	return cp.Files, nil
}

func validateFiles(files map[string]Image) error {
	if len(files) != 2 || files["source"] == nil || files["connection"] == nil {
		return errors.New("complete source and journal recovery state required")
	}
	var size int
	for _, data := range files {
		if len(data) > 32 {
			return errors.New("recovery bucket budget exceeded")
		}
		for b, values := range data {
			if len(b) == 0 || len(b) > 128 || len(values) > 20000 {
				return errors.New("invalid recovery bucket")
			}
			for k, v := range values {
				if len(k) == 0 || len(k) > 4096 || len(v) > 32<<20 {
					return errors.New("invalid recovery value")
				}
				size += len(v) + len(k) + len(b)
				if size > MaxRecoveryBytes {
					return errors.New("recovery state budget exceeded")
				}
			}
		}
	}
	return nil
}

func (d *DB) Attach(r *Replica, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failed || d.remote != nil || r.failed.Load() || r.etag == "" || r.files[name] == nil {
		return ErrUnavailable
	}
	var local Image
	err := d.db.View(func(tx *bolt.Tx) error { var err error; local, err = image(tx); return err })
	if err != nil || !reflect.DeepEqual(local, r.files[name]) {
		return errors.New("local state does not match remote recovery revision")
	}
	d.remote, d.name = r, name
	return nil
}

func (r *Replica) commit(name string, tx *bolt.Tx) error {
	next, err := image(tx)
	if err != nil {
		r.failed.Store(true)
		return ErrUnavailable
	}
	delta := changes(r.files[name], next)
	if len(delta) == 0 {
		return nil
	}
	var payloadBytes uint64
	for _, c := range delta {
		payloadBytes += uint64(len(c.Data))
	}
	if payloadBytes > MaxRecoveryBytes {
		r.failed.Store(true)
		return ErrUnavailable
	}
	if r.head.Depth >= maxReplayRecords || r.head.ReplayBytes+payloadBytes > MaxRecoveryBytes {
		if err := r.publishCheckpoint(); err != nil {
			r.failed.Store(true)
			return ErrUnavailable
		}
	}
	candidate := map[string]Image{"source": r.files["source"], "connection": r.files["connection"]}
	candidate[name] = next
	if validateFiles(candidate) != nil || r.head.StateRevision >= math.MaxInt64 || r.head.Revision >= math.MaxInt64 {
		r.failed.Store(true)
		return ErrUnavailable
	}
	entry := record{Format: 1, Binding: r.head.Binding, Revision: r.head.StateRevision + 1, Previous: r.head.Tail, File: name, Changes: delta}
	ref, size, err := r.upload("records", entry)
	if err == nil {
		h := r.head
		h.Tail, h.StateRevision, h.Depth, h.Revision = ref, entry.Revision, h.Depth+1, h.Revision+1
		h.ReplayBytes += payloadBytes
		h.Bytes += uint64(size)
		err = r.putHead(h, r.etag)
	}
	if err != nil {
		r.failed.Store(true)
		return ErrUnavailable
	}
	r.files = candidate
	return nil
}

func (r *Replica) publishCheckpoint() error {
	ref, size, err := r.upload("checkpoints", checkpoint{Format: 1, Binding: r.head.Binding, Revision: r.head.StateRevision, Files: r.files})
	if err != nil {
		return err
	}
	h := r.head
	h.Checkpoint, h.CheckpointRevision = ref, h.StateRevision
	h.Tail, h.Depth, h.Revision = reference{}, 0, h.Revision+1
	h.ReplayBytes = 0
	h.Bytes += uint64(size)
	return r.putHead(h, r.etag)
}

func (r *Replica) putHead(h head, etag string) error {
	if h.Bytes > maxRetainedBytes || h.Revision == 0 || h.Revision >= math.MaxInt64 {
		return errors.New("remote retained-state budget exceeded")
	}
	raw, err := json.Marshal(h)
	if err != nil {
		return err
	}
	next, err := r.objects.Put(r.ctx, r.root+"head.json", raw, etag)
	if err != nil || next == "" {
		return errors.New("remote state publication unconfirmed; stop and inspect")
	}
	r.head, r.etag = h, next
	return nil
}

func (r *Replica) upload(kind string, value any) (reference, int, error) {
	var buffer bytes.Buffer
	gz := gzip.NewWriter(&buffer)
	if err := json.NewEncoder(gz).Encode(value); err != nil {
		return reference{}, 0, err
	}
	if err := gz.Close(); err != nil {
		return reference{}, 0, err
	}
	raw := buffer.Bytes()
	if len(raw) > maxObjectBytes || r.head.Bytes+uint64(len(raw)) > maxRetainedBytes {
		return reference{}, 0, errors.New("remote state budget exceeded")
	}
	id, err := Token()
	if err != nil {
		return reference{}, 0, err
	}
	key := kind + "/" + id
	if _, err = r.objects.Put(r.ctx, r.root+key, raw, ""); err != nil {
		return reference{}, 0, errors.New("remote state upload unconfirmed")
	}
	return reference{Key: key, Digest: digest(raw)}, len(raw), nil
}

func (r *Replica) read(ref reference, kind string, out any) error {
	if !strings.HasPrefix(ref.Key, kind+"/") || !hexID.MatchString(strings.TrimPrefix(ref.Key, kind+"/")) || !hashID.MatchString(ref.Digest) {
		return errors.New("invalid recovery object reference")
	}
	raw, _, err := r.objects.Get(r.ctx, r.root+ref.Key, maxObjectBytes)
	if err != nil || digest(raw) != ref.Digest {
		return errors.New("recovery object missing or corrupt")
	}
	gz, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return errors.New("invalid recovery object encoding")
	}
	defer gz.Close()
	reader := &io.LimitedReader{R: gz, N: 2*MaxRecoveryBytes + 1}
	decoder := json.NewDecoder(reader)
	if err := decoder.Decode(out); err != nil || reader.N <= 0 {
		return errors.New("invalid or oversized recovery object")
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF || reader.N <= 0 {
		return errors.New("invalid recovery object trailer")
	}
	return nil
}
