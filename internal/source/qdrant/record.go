package qdrant

import (
	"strconv"

	qdrantapi "github.com/qdrant/go-client/qdrant"

	"github.com/lambdadb/lambdadb-migration/internal/source"
)

func retrievedPointToRecord(point *qdrantapi.RetrievedPoint) source.Record {
	return source.Record{
		ID:      pointIDToString(point.GetId()),
		Payload: payloadToMap(point.GetPayload()),
		Vectors: vectorsOutputToMap(point.GetVectors()),
	}
}

func pointIDToString(id *qdrantapi.PointId) string {
	if id == nil {
		return ""
	}
	if uuid := id.GetUuid(); uuid != "" {
		return uuid
	}
	return strconv.FormatUint(id.GetNum(), 10)
}

func payloadToMap(payload map[string]*qdrantapi.Value) map[string]any {
	out := make(map[string]any, len(payload))
	for key, value := range payload {
		out[key] = valueToAny(value)
	}
	return out
}

func valueToAny(value *qdrantapi.Value) any {
	if value == nil {
		return nil
	}
	switch v := value.GetKind().(type) {
	case *qdrantapi.Value_NullValue:
		return nil
	case *qdrantapi.Value_DoubleValue:
		return v.DoubleValue
	case *qdrantapi.Value_IntegerValue:
		return v.IntegerValue
	case *qdrantapi.Value_StringValue:
		return v.StringValue
	case *qdrantapi.Value_BoolValue:
		return v.BoolValue
	case *qdrantapi.Value_StructValue:
		out := make(map[string]any, len(v.StructValue.GetFields()))
		for key, nested := range v.StructValue.GetFields() {
			out[key] = valueToAny(nested)
		}
		return out
	case *qdrantapi.Value_ListValue:
		out := make([]any, 0, len(v.ListValue.GetValues()))
		for _, item := range v.ListValue.GetValues() {
			out = append(out, valueToAny(item))
		}
		return out
	default:
		return nil
	}
}

func vectorsOutputToMap(vectors *qdrantapi.VectorsOutput) map[string]source.VectorValue {
	out := map[string]source.VectorValue{}
	if vectors == nil {
		return out
	}
	if vector := vectors.GetVector(); vector != nil {
		out[""] = vectorOutputToValue(vector)
		return out
	}
	if named := vectors.GetVectors(); named != nil {
		for name, vector := range named.GetVectors() {
			out[name] = vectorOutputToValue(vector)
		}
	}
	return out
}

func vectorOutputToValue(vector *qdrantapi.VectorOutput) source.VectorValue {
	if dense := vector.GetDense(); dense != nil {
		return source.VectorValue{Dense: dense.GetData()}
	}
	if sparse := vector.GetSparse(); sparse != nil {
		return source.VectorValue{Sparse: sparseToMap(sparse)}
	}
	if multi := vector.GetMultiDense(); multi != nil {
		rows := make([][]float32, 0, len(multi.GetVectors()))
		for _, row := range multi.GetVectors() {
			rows = append(rows, row.GetData())
		}
		return source.VectorValue{Multi: rows}
	}
	return source.VectorValue{}
}

func sparseToMap(sparse *qdrantapi.SparseVector) map[string]float32 {
	indices := sparse.GetIndices()
	values := sparse.GetValues()
	out := make(map[string]float32, len(indices))
	for i, index := range indices {
		if i >= len(values) {
			break
		}
		out[strconv.FormatUint(uint64(index), 10)] = values[i]
	}
	return out
}
