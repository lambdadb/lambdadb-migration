package lambdadb

import (
	"encoding/json"
	"fmt"

	"github.com/lambdadb/lambdadb-migration/internal/config"
)

const (
	regularUpsertMaxPayloadBytes int64 = 6_000_000
	bulkUpsertMaxPayloadBytes    int64 = 200_000_000
)

func EffectiveMaxBatchBytes(requested int64, writeMode config.WriteMode) int64 {
	limit := bulkUpsertMaxPayloadBytes
	if writeMode == config.WriteModeUpsert {
		limit = regularUpsertMaxPayloadBytes
	}
	if requested > limit {
		return limit
	}
	return requested
}

func SplitDocumentsByPayloadSize(docs []map[string]any, maxBytes int64) ([][]map[string]any, error) {
	if len(docs) == 0 {
		return nil, nil
	}

	batches := make([][]map[string]any, 0, 1)
	current := make([]map[string]any, 0, len(docs))
	currentBytes := emptyUpsertPayloadBytes()

	for i, doc := range docs {
		docBytes, err := json.Marshal(doc)
		if err != nil {
			return nil, fmt.Errorf("encode document %d for batch sizing: %w", i, err)
		}
		singleBytes := upsertPayloadBytes([]int{len(docBytes)})
		if singleBytes > maxBytes {
			id, _ := doc["id"].(string)
			if id == "" {
				id = fmt.Sprintf("#%d", i)
			}
			return nil, fmt.Errorf("document %s serialized payload is %d bytes, exceeding max batch bytes %d", id, singleBytes, maxBytes)
		}

		nextBytes := currentBytes + int64(len(docBytes))
		if len(current) > 0 {
			nextBytes++
		}
		if nextBytes > maxBytes {
			batches = append(batches, current)
			current = make([]map[string]any, 0, len(docs)-i)
			currentBytes = emptyUpsertPayloadBytes()
			nextBytes = singleBytes
		}

		current = append(current, doc)
		currentBytes = nextBytes
	}

	if len(current) > 0 {
		batches = append(batches, current)
	}
	return batches, nil
}

func emptyUpsertPayloadBytes() int64 {
	return int64(len(`{"docs":[]}`))
}

func upsertPayloadBytes(docBytes []int) int64 {
	total := emptyUpsertPayloadBytes()
	for i, size := range docBytes {
		total += int64(size)
		if i > 0 {
			total++
		}
	}
	return total
}
