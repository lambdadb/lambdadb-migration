// Package stateobjects provides an isolated conditional object store for tests.
package stateobjects

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/lambdadb/lambdadb-migration/internal/statefile"
)

type Object struct {
	Data []byte
	ETag string
}

type Store struct {
	mu     sync.Mutex
	values map[string]Object
	serial int
	Fault  func(key string, after bool) error
	Puts   int
	Bytes  int
}

func (s *Store) SetFault(fn func(string, bool) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Fault = fn
}

func (s *Store) Counters() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Puts, s.Bytes
}

func New() *Store { return &Store{values: map[string]Object{}} }

func (s *Store) Get(_ context.Context, key string, limit int64) ([]byte, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.values[key]
	if !ok {
		return nil, "", statefile.ErrNotFound
	}
	if int64(len(v.Data)) > limit {
		return nil, "", errors.New("fixture object exceeds read limit")
	}
	return bytes.Clone(v.Data), v.ETag, nil
}

func (s *Store) Put(_ context.Context, key string, raw []byte, expected string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Fault != nil {
		if err := s.Fault(key, false); err != nil {
			return "", err
		}
	}
	v, exists := s.values[key]
	if (expected == "" && exists) || (expected != "" && (!exists || v.ETag != expected)) {
		return "", statefile.ErrConflict
	}
	s.serial++
	etag := fmt.Sprintf("\"etag-%d\"", s.serial)
	s.values[key] = Object{Data: bytes.Clone(raw), ETag: etag}
	s.Puts++
	s.Bytes += len(raw)
	if s.Fault != nil {
		if err := s.Fault(key, true); err != nil {
			return "", err
		}
	}
	return etag, nil
}

// Corrupt is deliberately not part of the production Objects contract.
func (s *Store) Corrupt(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.values[key]
	v.Data = []byte("corrupt-object")
	s.values[key] = v
}
