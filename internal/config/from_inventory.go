package config

import "github.com/lambdadb/lambdadb-migration/internal/source"

func MappingFromInventory(inv *source.Inventory, targetCollection string) MappingConfig {
	mapping := MappingConfig{
		Target: MappingTarget{
			Collection:       targetCollection,
			CreateCollection: true,
		},
		Vectors:       map[string]VectorMapping{},
		SparseVectors: map[string]SparseVectorMapping{},
		Payload: PayloadMapping{
			Mode:         "flatten",
			Rename:       map[string]string{},
			IndexConfigs: map[string]map[string]any{},
		},
		IDs: IDMapping{
			TargetField: "id",
		},
	}

	for sourceName, vector := range inv.Vectors {
		mapping.Vectors[sourceName] = VectorMapping{
			TargetField: NormalizeFieldName(vector.Name),
			Dimensions:  vector.Dimensions,
			Similarity:  vector.Similarity,
		}
	}
	for sourceName, sparse := range inv.SparseVectors {
		mapping.SparseVectors[sourceName] = SparseVectorMapping{
			TargetField: NormalizeFieldName(sparse.Name),
		}
	}
	for name, index := range inv.PayloadIndexes {
		targetName := NormalizeFieldName(name)
		if targetName != name {
			mapping.Payload.Rename[name] = targetName
		}
		mapping.Payload.IndexConfigs[targetName] = map[string]any{
			"type": index.Type,
		}
	}
	return mapping
}
