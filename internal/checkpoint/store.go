package checkpoint

import (
	"context"
	"time"
)

type Checkpoint struct {
	SourceKind       string `json:"sourceKind"`
	SourceCollection string `json:"sourceCollection"`
	TargetCollection string `json:"targetCollection"`
	Cursor           any    `json:"cursor,omitempty"`
	AcceptedRecords  uint64 `json:"acceptedRecords"`
	// SourceDone records source exhaustion after all writes have been accepted.
	// It does not mean post-migration validation has passed.
	SourceDone        bool             `json:"sourceDone,omitempty"`
	ValidationSamples []map[string]any `json:"validationSamples,omitempty"`
	UpdatedAt         time.Time        `json:"updatedAt"`
}

type Store interface {
	Load(ctx context.Context, key string) (*Checkpoint, error)
	Save(ctx context.Context, key string, checkpoint Checkpoint) error
	Delete(ctx context.Context, key string) error
}
