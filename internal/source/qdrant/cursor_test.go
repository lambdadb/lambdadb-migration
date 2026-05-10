package qdrant

import (
	"encoding/json"
	"strings"
	"testing"

	qdrantapi "github.com/qdrant/go-client/qdrant"

	"github.com/lambdadb/lambdadb-migration/internal/source"
)

func TestPointIDToCursorStoresNumericIDAsString(t *testing.T) {
	const maxID = uint64(18446744073709551615)

	cursor := pointIDToCursor(qdrantapi.NewIDNum(maxID))
	data, err := json.Marshal(cursor)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if got, want := string(data), `"num":"18446744073709551615"`; !strings.Contains(got, want) {
		t.Fatalf("cursor JSON = %s, want it to contain %s", got, want)
	}

	var reloaded map[string]any
	if err := json.Unmarshal(data, &reloaded); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	pointID, err := cursorToPointID(source.Cursor{Value: reloaded})
	if err != nil {
		t.Fatalf("cursorToPointID() error = %v", err)
	}
	if got := pointID.GetNum(); got != maxID {
		t.Fatalf("cursor numeric ID = %d, want %d", got, maxID)
	}
}

func TestCursorToPointIDAcceptsCurrentTypedCursor(t *testing.T) {
	const id = uint64(42)

	pointID, err := cursorToPointID(source.Cursor{Value: pointIDToCursor(qdrantapi.NewIDNum(id))})
	if err != nil {
		t.Fatalf("cursorToPointID() error = %v", err)
	}
	if got := pointID.GetNum(); got != id {
		t.Fatalf("cursor numeric ID = %d, want %d", got, id)
	}
}
