package config

import "testing"

func TestDecodeMappingYAMLDirect(t *testing.T) {
	mapping, err := DecodeMapping([]byte(`
target:
  collection: articles
  createCollection: true
vectors:
  "":
    targetField: dense
    dimensions: 1536
    similarity: cosine
ids:
  targetField: id
`))
	if err != nil {
		t.Fatalf("DecodeMapping() error = %v", err)
	}
	if got, want := mapping.Target.Collection, "articles"; got != want {
		t.Fatalf("target collection = %q, want %q", got, want)
	}
	if got, want := mapping.Vectors[""].TargetField, "dense"; got != want {
		t.Fatalf("vector target = %q, want %q", got, want)
	}
}

func TestDecodeMappingYAMLWrappedInventoryOutput(t *testing.T) {
	mapping, err := DecodeMapping([]byte(`
inventory:
  sourceKind: qdrant
mapping:
  target:
    collection: articles
  payload:
    mode: flatten
`))
	if err != nil {
		t.Fatalf("DecodeMapping() error = %v", err)
	}
	if got, want := mapping.Target.Collection, "articles"; got != want {
		t.Fatalf("target collection = %q, want %q", got, want)
	}
}

func TestDecodeMappingJSONStillWorks(t *testing.T) {
	mapping, err := DecodeMapping([]byte(`{"target":{"collection":"articles"}}`))
	if err != nil {
		t.Fatalf("DecodeMapping() error = %v", err)
	}
	if got, want := mapping.Target.Collection, "articles"; got != want {
		t.Fatalf("target collection = %q, want %q", got, want)
	}
}
