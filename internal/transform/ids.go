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
	doc[idField] = record.ID
	if mapping.IDs.CopyOriginalTo != "" && mapping.IDs.CopyOriginalTo != idField {
		doc[mapping.IDs.CopyOriginalTo] = record.ID
	}

	for key, value := range record.Payload {
		targetKey := key
		if renamed, ok := mapping.Payload.Rename[key]; ok {
			targetKey = renamed
		}
		doc[targetKey] = value
	}

	for sourceName, vector := range record.Vectors {
		if vector.Dense != nil {
			targetName, err := denseVectorTarget(sourceName, mapping)
			if err != nil {
				return nil, err
			}
			doc[targetName] = vector.Dense
			continue
		}
		if vector.Sparse != nil {
			targetName, err := sparseVectorTarget(sourceName, mapping)
			if err != nil {
				return nil, err
			}
			doc[targetName] = vector.Sparse
			continue
		}
		if vector.Multi != nil {
			return nil, fmt.Errorf("source vector %q is a multi-vector and requires custom migration handling", sourceName)
		}
	}

	return doc, nil
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
