package elasticsearch

import (
	"context"
	"fmt"
	"net/http"
	"sort"

	"github.com/lambdadb/lambdadb-migration/internal/source"
)

func (s *Source) Inventory(ctx context.Context) (*source.Inventory, error) {
	if s.inventory != nil {
		return cloneInventory(s.inventory), nil
	}
	var mappings mappingResponse
	if err := s.do(ctx, http.MethodGet, "/"+pathEscape(s.cfg.Index)+"/_mapping", nil, &mappings); err != nil {
		return nil, fmt.Errorf("get elasticsearch mapping: %w", err)
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

	indexNames := make([]string, 0, len(mappings))
	for name := range mappings {
		indexNames = append(indexNames, name)
	}
	sort.Strings(indexNames)
	if len(indexNames) == 0 {
		return nil, fmt.Errorf("elasticsearch mapping response did not include index mappings")
	}
	if len(indexNames) > 1 {
		inv.Warnings = append(inv.Warnings, fmt.Sprintf("mapping request returned %d indexes; using %q for generated mapping", len(indexNames), indexNames[0]))
	}
	walkProperties(inv, "", mappings[indexNames[0]].Mappings.Properties)
	if len(inv.Vectors) == 0 {
		inv.Warnings = append(inv.Warnings, "source index does not expose dense_vector fields")
	}
	if len(inv.PayloadIndexes) == 0 {
		inv.Warnings = append(inv.Warnings, "source index does not expose LambdaDB-compatible payload fields")
	}
	inv.Warnings = append(inv.Warnings, "Elasticsearch PIT checkpoints can expire; if a resumed migration fails with an expired PIT, rerun with --migration.restart")
	s.inventory = cloneInventory(inv)
	return inv, nil
}

func walkProperties(inv *source.Inventory, prefix string, properties map[string]fieldMapping) {
	names := make([]string, 0, len(properties))
	for name := range properties {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		field := properties[name]
		path := name
		if prefix != "" {
			path = prefix + "." + name
		}
		if field.Type == "" && len(field.Properties) > 0 {
			walkProperties(inv, path, field.Properties)
			continue
		}
		switch field.Type {
		case "dense_vector":
			if field.Dimensions < 1 {
				inv.Warnings = append(inv.Warnings, fmt.Sprintf("dense_vector field %q did not report dims; add dimensions manually before migration", path))
			}
			inv.Vectors[path] = source.VectorField{
				Name:       path,
				Dimensions: field.Dimensions,
				Similarity: mapSimilarity(field.Similarity),
			}
		case "text":
			inv.PayloadIndexes[path] = source.PayloadIndex{Name: path, Type: "text"}
		case "keyword", "constant_keyword", "wildcard":
			inv.PayloadIndexes[path] = source.PayloadIndex{Name: path, Type: "keyword"}
		case "long", "integer", "short", "byte", "unsigned_long":
			inv.PayloadIndexes[path] = source.PayloadIndex{Name: path, Type: "long"}
		case "double", "float", "half_float", "scaled_float":
			inv.PayloadIndexes[path] = source.PayloadIndex{Name: path, Type: "double"}
		case "date", "date_nanos":
			inv.PayloadIndexes[path] = source.PayloadIndex{Name: path, Type: "datetime"}
		case "boolean":
			inv.PayloadIndexes[path] = source.PayloadIndex{Name: path, Type: "boolean"}
		case "object":
			if len(field.Properties) > 0 {
				walkProperties(inv, path, field.Properties)
			} else {
				inv.PayloadIndexes[path] = source.PayloadIndex{Name: path, Type: "object"}
			}
		case "nested":
			inv.Warnings = append(inv.Warnings, fmt.Sprintf("nested field %q requires query rewrite review; migrating nested values as stored JSON payload fields", path))
			inv.PayloadIndexes[path] = source.PayloadIndex{Name: path, Type: "object"}
		case "":
			// Dynamic or disabled fields can still be stored in _source, but cannot produce an index config.
		default:
			inv.Warnings = append(inv.Warnings, fmt.Sprintf("Elasticsearch field %q uses unsupported mapping type %q; it will be stored but not indexed by generated mapping", path, field.Type))
		}
		if len(field.Fields) > 0 {
			inv.Warnings = append(inv.Warnings, fmt.Sprintf("Elasticsearch multi-fields under %q are not copied as separate source fields; add explicit LambdaDB fields if needed", path))
		}
	}
}

func mapSimilarity(value string) string {
	switch value {
	case "", "cosine":
		return "cosine"
	case "l2_norm":
		return "euclidean"
	case "dot_product":
		return "dot_product"
	case "max_inner_product":
		return "max_inner_product"
	default:
		return "unknown"
	}
}

func cloneInventory(inv *source.Inventory) *source.Inventory {
	if inv == nil {
		return nil
	}
	out := *inv
	out.Vectors = make(map[string]source.VectorField, len(inv.Vectors))
	for key, value := range inv.Vectors {
		out.Vectors[key] = value
	}
	out.SparseVectors = make(map[string]source.SparseVectorField, len(inv.SparseVectors))
	for key, value := range inv.SparseVectors {
		out.SparseVectors[key] = value
	}
	out.PayloadIndexes = make(map[string]source.PayloadIndex, len(inv.PayloadIndexes))
	for key, value := range inv.PayloadIndexes {
		out.PayloadIndexes[key] = value
	}
	out.Warnings = append([]string(nil), inv.Warnings...)
	return &out
}

type mappingResponse map[string]indexMapping

type indexMapping struct {
	Mappings typeMapping `json:"mappings"`
}

type typeMapping struct {
	Properties map[string]fieldMapping `json:"properties"`
}

type fieldMapping struct {
	Type       string                  `json:"type"`
	Dimensions int64                   `json:"dims"`
	Similarity string                  `json:"similarity"`
	Analyzer   string                  `json:"analyzer"`
	Properties map[string]fieldMapping `json:"properties"`
	Fields     map[string]fieldMapping `json:"fields"`
}
