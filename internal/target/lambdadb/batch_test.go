package lambdadb

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/lambdadb/lambdadb-migration/internal/config"
)

func TestEffectiveMaxBatchBytesCapsUpsertLimit(t *testing.T) {
	got := EffectiveMaxBatchBytes(200_000_000, config.WriteModeUpsert)
	if got != regularUpsertMaxPayloadBytes {
		t.Fatalf("EffectiveMaxBatchBytes() = %d, want %d", got, regularUpsertMaxPayloadBytes)
	}
}

func TestSplitDocumentsByPayloadSize(t *testing.T) {
	docs := []map[string]any{
		{"id": "1", "body": strings.Repeat("a", 10)},
		{"id": "2", "body": strings.Repeat("b", 10)},
		{"id": "3", "body": strings.Repeat("c", 10)},
	}
	firstDocSize := mustMarshalLen(t, docs[0])
	secondDocSize := mustMarshalLen(t, docs[1])
	maxBytes := upsertPayloadBytes([]int{firstDocSize, secondDocSize})

	batches, err := SplitDocumentsByPayloadSize(docs, maxBytes)
	if err != nil {
		t.Fatalf("SplitDocumentsByPayloadSize() error = %v", err)
	}
	if got, want := len(batches), 2; got != want {
		t.Fatalf("batch count = %d, want %d", got, want)
	}
	if got, want := len(batches[0]), 2; got != want {
		t.Fatalf("first batch size = %d, want %d", got, want)
	}
	if got, want := len(batches[1]), 1; got != want {
		t.Fatalf("second batch size = %d, want %d", got, want)
	}
}

func TestSplitDocumentsByPayloadSizeRejectsSingleOversizedDocument(t *testing.T) {
	docs := []map[string]any{{"id": "large", "body": "abcdef"}}
	maxBytes := upsertPayloadBytes([]int{mustMarshalLen(t, docs[0])}) - 1

	_, err := SplitDocumentsByPayloadSize(docs, maxBytes)
	if err == nil {
		t.Fatal("SplitDocumentsByPayloadSize() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "large") {
		t.Fatalf("error = %q, want document id", err.Error())
	}
}

func mustMarshalLen(t *testing.T, value any) int {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	return len(data)
}
