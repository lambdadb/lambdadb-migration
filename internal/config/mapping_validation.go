package config

import (
	"errors"
	"fmt"
	"strings"

	"github.com/lambdadb/lambdadb-migration/internal/source"
)

const MaxLambdaDBVectorDimensions int64 = 4096

func ValidateMapping(inv *source.Inventory, mapping MappingConfig, targetCollection string, writeMode WriteMode) error {
	var errs []error
	fields := newFieldRegistry()

	if inv == nil {
		errs = append(errs, fmt.Errorf("source inventory is required"))
	} else if inv.CollectionName == "" {
		errs = append(errs, fmt.Errorf("source inventory collection name is required"))
	}
	if mapping.Target.Collection == "" {
		errs = append(errs, fmt.Errorf("mapping target.collection is required"))
	} else if targetCollection != "" && mapping.Target.Collection != targetCollection {
		errs = append(errs, fmt.Errorf("mapping target.collection %q does not match CLI target collection %q", mapping.Target.Collection, targetCollection))
	}
	if writeMode != WriteModeBulk && writeMode != WriteModeUpsert {
		errs = append(errs, fmt.Errorf("unsupported write mode %q", writeMode))
	}

	errs = append(errs, validateIDMapping(mapping.IDs, fields)...)
	if inv != nil {
		errs = append(errs, validateVectorMappings(inv.Vectors, mapping.Vectors, fields)...)
		errs = append(errs, validateSparseVectorMappings(inv.SparseVectors, mapping.SparseVectors, fields)...)
	}
	errs = append(errs, validatePayloadMapping(inv, mapping.Payload, fields)...)

	return errors.Join(errs...)
}

func validateIDMapping(ids IDMapping, fields *fieldRegistry) []error {
	var errs []error
	targetField := ids.TargetField
	if targetField == "" {
		targetField = "id"
	}
	if strings.Contains(targetField, ".") {
		errs = append(errs, fmt.Errorf("id target field %q contains '.', which LambdaDB field names do not support", targetField))
	} else {
		errs = append(errs, fields.add(targetField, "document id")...)
	}
	if ids.CopyOriginalTo != "" && strings.Contains(ids.CopyOriginalTo, ".") {
		errs = append(errs, fmt.Errorf("id copyOriginalTo field %q contains '.', which LambdaDB field names do not support", ids.CopyOriginalTo))
	} else if ids.CopyOriginalTo != "" {
		errs = append(errs, fields.add(ids.CopyOriginalTo, "copied source id")...)
	}
	return errs
}

func validateVectorMappings(inventory map[string]source.VectorField, mappings map[string]VectorMapping, fields *fieldRegistry) []error {
	var errs []error
	for sourceName, vector := range inventory {
		mapping, ok := mappings[sourceName]
		if !ok {
			errs = append(errs, fmt.Errorf("missing vector mapping for source vector %q", displaySourceField(sourceName)))
			continue
		}
		errs = append(errs, validateVectorMapping(sourceName, vector, mapping)...)
		if mapping.TargetField != "" && !strings.Contains(mapping.TargetField, ".") {
			errs = append(errs, fields.add(mapping.TargetField, fmt.Sprintf("vector %q", displaySourceField(sourceName)))...)
		}
	}
	for sourceName := range mappings {
		if _, ok := inventory[sourceName]; !ok {
			errs = append(errs, fmt.Errorf("vector mapping %q does not exist in source inventory", displaySourceField(sourceName)))
		}
	}
	return errs
}

func validateVectorMapping(sourceName string, vector source.VectorField, mapping VectorMapping) []error {
	var errs []error
	name := displaySourceField(sourceName)
	if mapping.TargetField == "" {
		errs = append(errs, fmt.Errorf("vector %q targetField is required", name))
	} else if strings.Contains(mapping.TargetField, ".") {
		errs = append(errs, fmt.Errorf("vector %q targetField %q contains '.', which LambdaDB field names do not support", name, mapping.TargetField))
	}
	if mapping.Dimensions < 1 {
		errs = append(errs, fmt.Errorf("vector %q dimensions must be greater than 0", name))
	} else if mapping.Dimensions > MaxLambdaDBVectorDimensions {
		errs = append(errs, fmt.Errorf("vector %q dimensions %d exceed LambdaDB limit %d", name, mapping.Dimensions, MaxLambdaDBVectorDimensions))
	}
	if vector.Dimensions > 0 && mapping.Dimensions != vector.Dimensions {
		errs = append(errs, fmt.Errorf("vector %q dimensions %d do not match source dimensions %d", name, mapping.Dimensions, vector.Dimensions))
	}
	if !isSupportedSimilarity(mapping.Similarity) {
		errs = append(errs, fmt.Errorf("vector %q uses unsupported similarity %q", name, mapping.Similarity))
	}
	return errs
}

func validateSparseVectorMappings(inventory map[string]source.SparseVectorField, mappings map[string]SparseVectorMapping, fields *fieldRegistry) []error {
	var errs []error
	for sourceName := range inventory {
		mapping, ok := mappings[sourceName]
		if !ok {
			errs = append(errs, fmt.Errorf("missing sparse vector mapping for source sparse vector %q", displaySourceField(sourceName)))
			continue
		}
		if mapping.TargetField == "" {
			errs = append(errs, fmt.Errorf("sparse vector %q targetField is required", displaySourceField(sourceName)))
		} else if strings.Contains(mapping.TargetField, ".") {
			errs = append(errs, fmt.Errorf("sparse vector %q targetField %q contains '.', which LambdaDB field names do not support", displaySourceField(sourceName), mapping.TargetField))
		} else {
			errs = append(errs, fields.add(mapping.TargetField, fmt.Sprintf("sparse vector %q", displaySourceField(sourceName)))...)
		}
	}
	for sourceName := range mappings {
		if _, ok := inventory[sourceName]; !ok {
			errs = append(errs, fmt.Errorf("sparse vector mapping %q does not exist in source inventory", displaySourceField(sourceName)))
		}
	}
	return errs
}

func validatePayloadMapping(inv *source.Inventory, payload PayloadMapping, fields *fieldRegistry) []error {
	var errs []error
	for sourceField, targetField := range payload.Rename {
		if targetField == "" {
			errs = append(errs, fmt.Errorf("payload rename for %q has empty target field", sourceField))
		} else if strings.Contains(targetField, ".") {
			errs = append(errs, fmt.Errorf("payload rename for %q targets %q, which contains '.'", sourceField, targetField))
		} else {
			errs = append(errs, fields.add(targetField, fmt.Sprintf("payload field %q", sourceField))...)
		}
	}
	if inv != nil {
		for sourceField := range inv.PayloadIndexes {
			targetField := PayloadTargetField(sourceField, payload.Rename)
			if strings.Contains(targetField, ".") {
				errs = append(errs, fmt.Errorf("payload field %q targets %q, which contains '.'", sourceField, targetField))
				continue
			}
			errs = append(errs, fields.add(targetField, fmt.Sprintf("payload field %q", sourceField))...)
		}
	}
	for field, index := range payload.IndexConfigs {
		if strings.Contains(field, ".") {
			errs = append(errs, fmt.Errorf("payload index field %q contains '.', rename it before creating a LambdaDB index", field))
		} else {
			errs = append(errs, fields.addPayloadIndex(field)...)
		}
		typ, _ := index["type"].(string)
		if !isSupportedPayloadIndexType(typ) {
			errs = append(errs, fmt.Errorf("payload field %q has unsupported index type %q", field, typ))
		}
		if typ == "text" {
			errs = append(errs, validateTextAnalyzers(field, index["analyzers"])...)
		}
	}
	return errs
}

func validateTextAnalyzers(field string, value any) []error {
	var errs []error
	switch analyzers := value.(type) {
	case nil:
		return nil
	case []string:
		for _, analyzer := range analyzers {
			if !isSupportedAnalyzer(analyzer) {
				errs = append(errs, fmt.Errorf("payload field %q has unsupported analyzer %q", field, analyzer))
			}
		}
	case []any:
		for _, raw := range analyzers {
			analyzer, ok := raw.(string)
			if !ok {
				errs = append(errs, fmt.Errorf("payload field %q has non-string analyzer %v", field, raw))
				continue
			}
			if !isSupportedAnalyzer(analyzer) {
				errs = append(errs, fmt.Errorf("payload field %q has unsupported analyzer %q", field, analyzer))
			}
		}
	default:
		errs = append(errs, fmt.Errorf("payload field %q analyzers must be a string array", field))
	}
	return errs
}

func isSupportedSimilarity(value string) bool {
	switch value {
	case "", "cosine", "euclidean", "dot_product", "max_inner_product":
		return true
	default:
		return false
	}
}

func isSupportedPayloadIndexType(value string) bool {
	switch value {
	case "keyword", "long", "double", "datetime", "boolean", "sparseVector", "text", "object":
		return true
	default:
		return false
	}
}

func isSupportedAnalyzer(value string) bool {
	switch value {
	case "standard", "english", "korean", "japanese":
		return true
	default:
		return false
	}
}

func displaySourceField(value string) string {
	if value == "" {
		return "<unnamed>"
	}
	return value
}

type fieldRegistry struct {
	fields map[string]string
}

func newFieldRegistry() *fieldRegistry {
	return &fieldRegistry{fields: map[string]string{}}
}

func (r *fieldRegistry) add(field, source string) []error {
	if field == "" {
		return nil
	}
	if existing, ok := r.fields[field]; ok {
		if existing == source {
			return nil
		}
		return []error{fmt.Errorf("field name collision: %s and %s both target %q", existing, source, field)}
	}
	r.fields[field] = source
	return nil
}

func (r *fieldRegistry) addPayloadIndex(field string) []error {
	if field == "" {
		return nil
	}
	if existing, ok := r.fields[field]; ok && strings.HasPrefix(existing, "payload field ") {
		return nil
	}
	return r.add(field, fmt.Sprintf("payload index %q", field))
}
