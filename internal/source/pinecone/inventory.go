package pinecone

import (
	"context"
	"fmt"

	pineconeapi "github.com/pinecone-io/go-pinecone/v5/pinecone"

	"github.com/lambdadb/lambdadb-migration/internal/source"
)

func (s *Source) Inventory(ctx context.Context) (*source.Inventory, error) {
	if err := s.ensureIndex(ctx); err != nil {
		return nil, err
	}
	count, err := s.Count(ctx)
	if err != nil {
		return nil, err
	}

	inv := &source.Inventory{
		SourceKind:     s.Name(),
		CollectionName: s.cfg.Index,
		RecordCount:    count,
		Vectors:        map[string]source.VectorField{},
		SparseVectors:  map[string]source.SparseVectorField{},
		PayloadIndexes: map[string]source.PayloadIndex{},
	}
	addVectorConfig(inv, s.index)
	if s.cfg.Namespace != "" {
		inv.Warnings = append(inv.Warnings, fmt.Sprintf("using Pinecone namespace %q", s.cfg.Namespace))
	}
	if s.cfg.ListPrefix != "" {
		inv.Warnings = append(inv.Warnings, fmt.Sprintf("migrating only Pinecone vector IDs with prefix %q", s.cfg.ListPrefix))
	}
	if s.index.Embed != nil {
		inv.Warnings = append(inv.Warnings, "Pinecone integrated embedding index detected; this connector migrates stored vector values and metadata, not model configuration")
	}
	if len(inv.PayloadIndexes) == 0 {
		inv.Warnings = append(inv.Warnings, "Pinecone metadata index settings are not introspected; payload fields will be stored without generated LambdaDB index configs")
	}
	return inv, nil
}

func addVectorConfig(inv *source.Inventory, index *pineconeapi.Index) {
	switch index.VectorType {
	case "sparse":
		inv.SparseVectors["sparse"] = source.SparseVectorField{Name: "sparse"}
	case "", "dense":
		if index.Dimension == nil {
			inv.Warnings = append(inv.Warnings, "Pinecone dense index did not report a dimension")
			return
		}
		inv.Vectors[""] = source.VectorField{
			Name:       "dense",
			Dimensions: int64(*index.Dimension),
			Similarity: mapMetric(index.Metric),
		}
	default:
		inv.Warnings = append(inv.Warnings, fmt.Sprintf("unknown Pinecone vector type %q", index.VectorType))
		if index.Dimension != nil {
			inv.Vectors[""] = source.VectorField{
				Name:       "dense",
				Dimensions: int64(*index.Dimension),
				Similarity: mapMetric(index.Metric),
			}
		}
	}
}

func mapMetric(metric pineconeapi.IndexMetric) string {
	switch metric {
	case pineconeapi.Cosine:
		return "cosine"
	case pineconeapi.Euclidean:
		return "euclidean"
	case pineconeapi.Dotproduct:
		return "dot_product"
	default:
		return "unknown"
	}
}
