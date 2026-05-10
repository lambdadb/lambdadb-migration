package source

import "context"

type Cursor struct {
	Value any `json:"value,omitempty"`
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
	SourceKind     string
	CollectionName string
	RecordCount    uint64
	Vectors        map[string]VectorField
	SparseVectors  map[string]SparseVectorField
	PayloadIndexes map[string]PayloadIndex
	Warnings       []string
}

type VectorField struct {
	Name       string
	Dimensions int64
	Similarity string
}

type SparseVectorField struct {
	Name string
}

type PayloadIndex struct {
	Name string
	Type string
}

type Source interface {
	Name() string
	Inventory(ctx context.Context) (*Inventory, error)
	Count(ctx context.Context) (uint64, error)
	Read(ctx context.Context, cursor Cursor, limit int) (Batch, error)
}
