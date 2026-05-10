package checkpoint

import (
	"context"
	"time"
)

type Checkpoint struct {
	SourceKind       string    `json:"sourceKind"`
	SourceCollection string    `json:"sourceCollection"`
	TargetCollection string    `json:"targetCollection"`
	Cursor           any       `json:"cursor,omitempty"`
	AcceptedRecords  uint64    `json:"acceptedRecords"`
	UpdatedAt        time.Time `json:"updatedAt"`
}

type Store interface {
	Load(ctx context.Context, key string) (*Checkpoint, error)
	Save(ctx context.Context, key string, checkpoint Checkpoint) error
	Delete(ctx context.Context, key string) error
}
