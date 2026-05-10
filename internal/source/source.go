package source

import "context"

type Cursor struct {
	Value any `json:"value,omitempty" yaml:"value,omitempty"`
}

type Batch struct {
	Records    []Record
	NextCursor *Cursor
	Done       bool
}

type Record struct {
	ID      string
	Payload map[string]any
	Vectors map[string]VectorValue
}

type VectorValue struct {
	Dense  []float32
	Sparse map[string]float32
	Multi  [][]float32
}

type Inventory struct {
	SourceKind     string                       `json:"sourceKind" yaml:"sourceKind"`
	CollectionName string                       `json:"collectionName" yaml:"collectionName"`
	RecordCount    uint64                       `json:"recordCount" yaml:"recordCount"`
	Vectors        map[string]VectorField       `json:"vectors" yaml:"vectors"`
	SparseVectors  map[string]SparseVectorField `json:"sparseVectors" yaml:"sparseVectors"`
	PayloadIndexes map[string]PayloadIndex      `json:"payloadIndexes" yaml:"payloadIndexes"`
	Warnings       []string                     `json:"warnings,omitempty" yaml:"warnings,omitempty"`
}

type VectorField struct {
	Name       string `json:"name" yaml:"name"`
	Dimensions int64  `json:"dimensions" yaml:"dimensions"`
	Similarity string `json:"similarity" yaml:"similarity"`
}

type SparseVectorField struct {
	Name string `json:"name" yaml:"name"`
}

type PayloadIndex struct {
	Name string `json:"name" yaml:"name"`
	Type string `json:"type" yaml:"type"`
}

type Source interface {
	Name() string
	Inventory(ctx context.Context) (*Inventory, error)
	Count(ctx context.Context) (uint64, error)
	Read(ctx context.Context, cursor Cursor, limit int) (Batch, error)
}
