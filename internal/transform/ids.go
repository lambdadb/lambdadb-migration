package transform

import (
	"fmt"

	"github.com/lambdadb/lambdadb-migration/internal/config"
	"github.com/lambdadb/lambdadb-migration/internal/source"
)

func RecordToDocument(record source.Record, copyOriginalIDTo string) map[string]any {
	doc := make(map[string]any, len(record.Payload)+len(record.Vectors)+2)
	doc["id"] = record.ID
	if copyOriginalIDTo != "" && copyOriginalIDTo != "id" {
		doc[copyOriginalIDTo] = record.ID
	}
	for key, value := range record.Payload {
		doc[key] = value
	}
	for key, value := range record.Vectors {
		if key == "" {
			key = "dense"
		}
		switch {
		case value.Dense != nil:
			doc[key] = value.Dense
		case value.Sparse != nil:
			doc[key] = value.Sparse
		case value.Multi != nil:
			doc[key] = value.Multi
		}
	}
	return doc
}

func RecordToDocumentWithMapping(record source.Record, mapping config.MappingConfig) (map[string]any, error) {
	idField := mapping.IDs.TargetField
	if idField == "" {
		idField = "id"
	}

	doc := make(map[string]any, len(record.Payload)+len(record.Vectors)+2)
	origins := map[string]string{}
	if err := setDocumentField(doc, origins, idField, record.ID, "document id"); err != nil {
		return nil, err
	}
	if mapping.IDs.CopyOriginalTo != "" && mapping.IDs.CopyOriginalTo != idField {
		if err := setDocumentField(doc, origins, mapping.IDs.CopyOriginalTo, record.ID, "copied source id"); err != nil {
			return nil, err
		}
	}

	for key, value := range record.Payload {
		targetKey := config.PayloadTargetField(key, mapping.Payload.Rename)
		if err := setDocumentField(doc, origins, targetKey, value, fmt.Sprintf("payload field %q", key)); err != nil {
			return nil, err
		}
	}

	for sourceName, vector := range record.Vectors {
		if vector.Dense != nil {
			targetName, err := denseVectorTarget(sourceName, mapping)
			if err != nil {
				return nil, err
			}
			if err := setDocumentField(doc, origins, targetName, vector.Dense, fmt.Sprintf("vector %q", displaySourceName(sourceName))); err != nil {
				return nil, err
			}
			continue
		}
		if vector.Sparse != nil {
			targetName, err := sparseVectorTarget(sourceName, mapping)
			if err != nil {
				return nil, err
			}
			if err := setDocumentField(doc, origins, targetName, vector.Sparse, fmt.Sprintf("sparse vector %q", displaySourceName(sourceName))); err != nil {
				return nil, err
			}
			continue
		}
		if vector.Multi != nil {
			return nil, fmt.Errorf("source vector %q is a multi-vector and requires custom migration handling", sourceName)
		}
	}

	return doc, nil
}

func setDocumentField(doc map[string]any, origins map[string]string, key string, value any, origin string) error {
	if existing, ok := origins[key]; ok {
		return fmt.Errorf("field name collision: %s and %s both target %q", existing, origin, key)
	}
	doc[key] = value
	origins[key] = origin
	return nil
}

func denseVectorTarget(sourceName string, mapping config.MappingConfig) (string, error) {
	if vector, ok := mapping.Vectors[sourceName]; ok && vector.TargetField != "" {
		return vector.TargetField, nil
	}
	if sourceName == "" {
		return "dense", nil
	}
	return "", fmt.Errorf("no dense vector mapping for source vector %q", sourceName)
}

func sparseVectorTarget(sourceName string, mapping config.MappingConfig) (string, error) {
	if vector, ok := mapping.SparseVectors[sourceName]; ok && vector.TargetField != "" {
		return vector.TargetField, nil
	}
	if sourceName != "" {
		return sourceName, nil
	}
	return "", fmt.Errorf("no sparse vector mapping for unnamed source vector")
}

func displaySourceName(value string) string {
	if value == "" {
		return "<unnamed>"
	}
	return value
}
