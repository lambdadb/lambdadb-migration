package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type FileStore struct {
	root string
}

func NewFileStore(root string) *FileStore {
	return &FileStore{root: root}
}

func (s *FileStore) Load(ctx context.Context, key string) (*Checkpoint, error) {
	path := s.path(key)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read checkpoint: %w", err)
	}
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("decode checkpoint: %w", err)
	}
	return &cp, nil
}

func (s *FileStore) Save(ctx context.Context, key string, checkpoint Checkpoint) error {
	path := s.path(key)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create checkpoint directory: %w", err)
	}
	data, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		return fmt.Errorf("encode checkpoint: %w", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write checkpoint: %w", err)
	}
	return nil
}

func (s *FileStore) Delete(ctx context.Context, key string) error {
	if err := os.Remove(s.path(key)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("delete checkpoint: %w", err)
	}
	return nil
}

func (s *FileStore) path(key string) string {
	return filepath.Join(s.root, key+".json")
}
