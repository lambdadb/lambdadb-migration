package elasticsearch

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/lambdadb/lambdadb-migration/internal/source"
)

type rawObject map[string]any

func (o *rawObject) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*o = rawObject{}
		return nil
	}
	var out map[string]any
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.UseNumber()
	if err := dec.Decode(&out); err != nil {
		return err
	}
	*o = out
	return nil
}

func hitToRecord(hit hit, vectorFields map[string]struct{}) (source.Record, error) {
	payload := map[string]any{}
	flattenPayload("", map[string]any(hit.Source), vectorFields, payload)

	vectors := map[string]source.VectorValue{}
	for field := range vectorFields {
		vector, ok, err := extractDenseVector(hit, field)
		if err != nil {
			return source.Record{}, err
		}
		if ok {
			vectors[field] = source.VectorValue{Dense: vector}
		}
	}

	return source.Record{
		ID:      hit.ID,
		Payload: payload,
		Vectors: vectors,
	}, nil
}

func flattenPayload(prefix string, value map[string]any, vectorFields map[string]struct{}, out map[string]any) {
	for key, item := range value {
		path := key
		if prefix != "" {
			path = prefix + "." + key
		}
		if _, isVector := vectorFields[path]; isVector {
			continue
		}
		if child, ok := item.(map[string]any); ok {
			flattenPayload(path, child, vectorFields, out)
			continue
		}
		out[path] = item
	}
}

func extractDenseVector(hit hit, field string) ([]float32, bool, error) {
	if value, ok := nestedValue(map[string]any(hit.Source), field); ok {
		vector, err := toFloat32Slice(value)
		if err != nil {
			return nil, false, fmt.Errorf("decode elasticsearch vector field %q from _source: %w", field, err)
		}
		return vector, true, nil
	}
	if hit.Fields == nil {
		return nil, false, nil
	}
	value, ok := hit.Fields[field]
	if !ok {
		return nil, false, nil
	}
	if values, ok := value.([]any); ok && len(values) == 1 {
		value = values[0]
	}
	vector, err := toFloat32Slice(value)
	if err != nil {
		return nil, false, fmt.Errorf("decode elasticsearch vector field %q from fields: %w", field, err)
	}
	return vector, true, nil
}

func nestedValue(root map[string]any, path string) (any, bool) {
	parts := strings.Split(path, ".")
	var current any = root
	for _, part := range parts {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[part]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func toFloat32Slice(value any) ([]float32, error) {
	values, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expected array, got %T", value)
	}
	out := make([]float32, 0, len(values))
	for _, raw := range values {
		number, err := toFloat32(raw)
		if err != nil {
			return nil, err
		}
		out = append(out, number)
	}
	return out, nil
}

func toFloat32(value any) (float32, error) {
	switch v := value.(type) {
	case float64:
		return float32(v), nil
	case float32:
		return v, nil
	case json.Number:
		parsed, err := strconv.ParseFloat(v.String(), 32)
		if err != nil {
			return 0, fmt.Errorf("parse vector value %q: %w", v.String(), err)
		}
		return float32(parsed), nil
	case int:
		return float32(v), nil
	case int64:
		return float32(v), nil
	default:
		return 0, fmt.Errorf("expected number, got %T", value)
	}
}
