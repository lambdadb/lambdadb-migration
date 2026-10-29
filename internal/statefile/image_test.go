package statefile

import (
	"bytes"
	"reflect"
	"testing"
)

func TestRecoverySplicesKeepExactBytesAndReuseSourceImage(t *testing.T) {
	old := append([]byte(`{"raw":9007199254740993,"content":{"z":1,"a":2},"body":"`), bytes.Repeat([]byte("unchanged-"), 2000)...)
	old = append(old, []byte(`","lsn":100}`)...)
	next := bytes.Replace(old, []byte(`"lsn":100`), []byte(`"lsn":200`), 1)
	before := Image{"source": {"state": old}}
	after := Image{"source": {"state": old}, "pending": {"next": next}}
	delta := changes(before, after)
	bytesWritten := 0
	for _, c := range delta {
		bytesWritten += len(c.Data)
	}
	if bytesWritten > 8 {
		t.Fatal("staged transaction copied the entire retained row image", bytesWritten)
	}
	got, err := applyChanges(before, delta)
	if err != nil || !reflect.DeepEqual(got, after) {
		t.Fatal("splice changed opaque request bytes or integer precision", err)
	}
	before["source"]["state"] = []byte("corrupt base")
	if _, err = applyChanges(before, delta); err == nil {
		t.Fatal("corrupt splice base accepted")
	}
}
