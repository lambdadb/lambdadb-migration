package statefile

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"

	bolt "go.etcd.io/bbolt"
)

// Image is a logical export, never a copy of an active mmap file. Managed state
// uses flat buckets; rejecting nested buckets avoids silently losing new state.
type Image map[string]map[string][]byte

const MaxRecoveryBytes = 128 << 20

func image(tx *bolt.Tx) (Image, error) {
	out := Image{}
	err := tx.ForEach(func(name []byte, b *bolt.Bucket) error {
		values := map[string][]byte{}
		out[string(name)] = values
		return b.ForEach(func(k, v []byte) error {
			if v == nil {
				return errors.New("nested recovery bucket unsupported")
			}
			values[string(k)] = bytes.Clone(v)
			return nil
		})
	})
	return out, err
}

func Export(db *DB) (Image, error) {
	var out Image
	err := db.View(func(tx *bolt.Tx) error { var err error; out, err = image(tx); return err })
	return out, err
}

func Import(tx *bolt.Tx, data Image) error {
	if len(data) > 32 {
		return errors.New("recovery bucket budget exceeded")
	}
	for bucket, values := range data {
		if len(bucket) == 0 || len(bucket) > 128 || len(values) > 20000 {
			return errors.New("invalid recovery bucket")
		}
		b, err := tx.CreateBucket([]byte(bucket))
		if err != nil {
			return err
		}
		for k, v := range values {
			if len(k) == 0 || len(k) > 4096 || len(v) > 32<<20 {
				return errors.New("invalid recovery value")
			}
			if err := b.Put([]byte(k), v); err != nil {
				return err
			}
		}
	}
	return nil
}

// A byte splice preserves exact request payloads/digests and JSON number bytes.
// New transaction images may use the retained source checkpoint as their base.
type change struct {
	Operation  string `json:"operation"`
	Bucket     string `json:"bucket"`
	Key        string `json:"key,omitempty"`
	BaseBucket string `json:"base_bucket,omitempty"`
	BaseKey    string `json:"base_key,omitempty"`
	Before     string `json:"before,omitempty"`
	After      string `json:"after,omitempty"`
	Prefix     int    `json:"prefix,omitempty"`
	Suffix     int    `json:"suffix,omitempty"`
	Data       []byte `json:"data,omitempty"`
}

func digest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func shared(a, b []byte) (int, int) {
	p, s := 0, 0
	for p < len(a) && p < len(b) && a[p] == b[p] {
		p++
	}
	for s < len(a)-p && s < len(b)-p && a[len(a)-s-1] == b[len(b)-s-1] {
		s++
	}
	return p, s
}

func changes(before, after Image) []change {
	var result []change
	for _, b := range keys(before) {
		if _, ok := after[b]; !ok {
			result = append(result, change{Operation: "drop_bucket", Bucket: b})
		}
	}
	for _, b := range keys(after) {
		if _, ok := before[b]; !ok {
			result = append(result, change{Operation: "create_bucket", Bucket: b})
		}
		for _, k := range keys(before[b]) {
			if _, ok := after[b][k]; !ok {
				result = append(result, change{Operation: "delete", Bucket: b, Key: k, Before: digest(before[b][k])})
			}
		}
		for _, k := range keys(after[b]) {
			raw := after[b][k]
			old, exists := before[b][k]
			if exists && bytes.Equal(old, raw) {
				continue
			}
			c := change{Operation: "put", Bucket: b, Key: k, After: digest(raw)}
			if exists {
				c.BaseBucket, c.BaseKey, c.Before = b, k, digest(old)
				c.Prefix, c.Suffix = shared(old, raw)
			} else if len(raw) > 4096 {
				// There are only a handful of state values. Reuse a complete prior
				// image when staging its changed successor in another bucket.
				for _, bb := range keys(before) {
					for _, kk := range keys(before[bb]) {
						p, s := shared(before[bb][kk], raw)
						if p+s > c.Prefix+c.Suffix {
							c.BaseBucket, c.BaseKey, c.Before = bb, kk, digest(before[bb][kk])
							c.Prefix, c.Suffix = p, s
						}
					}
				}
			}
			c.Data = bytes.Clone(raw[c.Prefix : len(raw)-c.Suffix])
			result = append(result, c)
		}
	}
	return result
}

func keys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func applyChanges(before Image, delta []change) (Image, error) {
	if len(delta) > 40000 {
		return nil, errors.New("recovery change budget exceeded")
	}
	out := Image{}
	for b, values := range before {
		out[b] = map[string][]byte{}
		for k, v := range values {
			out[b][k] = v
		}
	}
	for _, c := range delta {
		switch c.Operation {
		case "create_bucket":
			if out[c.Bucket] != nil {
				return nil, errors.New("recovery bucket already exists")
			}
			out[c.Bucket] = map[string][]byte{}
		case "drop_bucket":
			if out[c.Bucket] == nil {
				return nil, errors.New("recovery bucket missing")
			}
			delete(out, c.Bucket)
		case "delete":
			if _, ok := out[c.Bucket][c.Key]; !ok || digest(out[c.Bucket][c.Key]) != c.Before {
				return nil, errors.New("recovery deletion base changed")
			}
			delete(out[c.Bucket], c.Key)
		case "put":
			if out[c.Bucket] == nil {
				return nil, errors.New("recovery bucket missing")
			}
			var base []byte
			if c.BaseBucket != "" {
				var ok bool
				base, ok = before[c.BaseBucket][c.BaseKey]
				if !ok || digest(base) != c.Before {
					return nil, errors.New("recovery patch base changed")
				}
			}
			if c.Prefix < 0 || c.Suffix < 0 || c.Prefix > len(base) || c.Suffix > len(base)-c.Prefix || len(c.Data) > 32<<20 || c.Prefix+c.Suffix+len(c.Data) > 32<<20 {
				return nil, errors.New("invalid recovery patch size")
			}
			size := c.Prefix + c.Suffix + len(c.Data) - len(out[c.Bucket][c.Key])
			for _, values := range out {
				for k, v := range values {
					size += len(k) + len(v)
				}
			}
			if size > MaxRecoveryBytes {
				return nil, errors.New("recovery image budget exceeded")
			}
			raw := make([]byte, 0, c.Prefix+len(c.Data)+c.Suffix)
			raw = append(raw, base[:c.Prefix]...)
			raw = append(raw, c.Data...)
			raw = append(raw, base[len(base)-c.Suffix:]...)
			if digest(raw) != c.After {
				return nil, errors.New("recovery patch checksum mismatch")
			}
			out[c.Bucket][c.Key] = raw
		default:
			return nil, errors.New("invalid recovery change")
		}
	}
	return out, nil
}
