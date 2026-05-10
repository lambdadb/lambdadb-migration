package qdrant

import (
	"context"
	"fmt"

	qdrantapi "github.com/qdrant/go-client/qdrant"

	"github.com/lambdadb/lambdadb-migration/internal/config"
	"github.com/lambdadb/lambdadb-migration/internal/source"
)

func (s *Source) Inventory(ctx context.Context) (*source.Inventory, error) {
	info, err := s.client.GetCollectionInfo(ctx, s.cfg.Collection)
	if err != nil {
		return nil, fmt.Errorf("get qdrant collection info: %w", err)
	}

	count, err := s.Count(ctx)
	if err != nil {
		return nil, err
	}

	inv := &source.Inventory{
		SourceKind:     s.Name(),
		CollectionName: s.cfg.Collection,
		RecordCount:    count,
		Vectors:        map[string]source.VectorField{},
		SparseVectors:  map[string]source.SparseVectorField{},
		PayloadIndexes: map[string]source.PayloadIndex{},
	}

	params := info.GetConfig().GetParams()
	if params != nil {
		addVectorConfig(inv, params.GetVectorsConfig())
		addSparseVectorConfig(inv, params.GetSparseVectorsConfig())
	}

	normalizedPayloadFields := map[string]string{}
	for name, schema := range info.GetPayloadSchema() {
		inv.PayloadIndexes[name] = source.PayloadIndex{
			Name: name,
			Type: mapPayloadType(schema.GetDataType()),
		}
		normalized := config.NormalizeFieldName(name)
		if normalized != name {
			inv.Warnings = append(inv.Warnings, fmt.Sprintf("payload field %q contains '.', suggested LambdaDB field name is %q", name, normalized))
		}
		if previous, ok := normalizedPayloadFields[normalized]; ok && previous != name {
			inv.Warnings = append(inv.Warnings, fmt.Sprintf("payload fields %q and %q both normalize to %q; add explicit payload renames before migration", previous, name, normalized))
		} else {
			normalizedPayloadFields[normalized] = name
		}
	}

	if len(inv.Vectors) == 0 && len(inv.SparseVectors) == 0 {
		inv.Warnings = append(inv.Warnings, "source collection does not expose vector configuration")
	}

	return inv, nil
}

func addVectorConfig(inv *source.Inventory, cfg *qdrantapi.VectorsConfig) {
	if cfg == nil {
		return
	}
	if params := cfg.GetParams(); params != nil {
		addVector(inv, "", params)
	}
	if paramsMap := cfg.GetParamsMap(); paramsMap != nil {
		for name, params := range paramsMap.GetMap() {
			addVector(inv, name, params)
		}
	}
}

func addVector(inv *source.Inventory, name string, params *qdrantapi.VectorParams) {
	fieldName := name
	if fieldName == "" {
		fieldName = "dense"
	}
	inv.Vectors[name] = source.VectorField{
		Name:       fieldName,
		Dimensions: int64(params.GetSize()),
		Similarity: mapDistance(params.GetDistance()),
	}
	if params.GetMultivectorConfig() != nil {
		inv.Warnings = append(inv.Warnings, fmt.Sprintf("vector %q uses Qdrant multi-vector config; migration requires custom handling", fieldName))
	}
	if params.GetDistance() == qdrantapi.Distance_Manhattan {
		inv.Warnings = append(inv.Warnings, fmt.Sprintf("vector %q uses Manhattan distance, which has no direct LambdaDB similarity equivalent", fieldName))
	}
}

func addSparseVectorConfig(inv *source.Inventory, cfg *qdrantapi.SparseVectorConfig) {
	if cfg == nil {
		return
	}
	for name := range cfg.GetMap() {
		inv.SparseVectors[name] = source.SparseVectorField{Name: name}
	}
}

func mapDistance(distance qdrantapi.Distance) string {
	switch distance {
	case qdrantapi.Distance_Cosine:
		return "cosine"
	case qdrantapi.Distance_Euclid:
		return "euclidean"
	case qdrantapi.Distance_Dot:
		return "dot_product"
	case qdrantapi.Distance_Manhattan:
		return "unsupported:manhattan"
	default:
		return "unknown"
	}
}

func mapPayloadType(typ qdrantapi.PayloadSchemaType) string {
	switch typ {
	case qdrantapi.PayloadSchemaType_Keyword, qdrantapi.PayloadSchemaType_Uuid:
		return "keyword"
	case qdrantapi.PayloadSchemaType_Integer:
		return "long"
	case qdrantapi.PayloadSchemaType_Float:
		return "double"
	case qdrantapi.PayloadSchemaType_Text:
		return "text"
	case qdrantapi.PayloadSchemaType_Bool:
		return "boolean"
	case qdrantapi.PayloadSchemaType_Datetime:
		return "datetime"
	case qdrantapi.PayloadSchemaType_Geo:
		return "unsupported:geo"
	default:
		return "unknown"
	}
}
