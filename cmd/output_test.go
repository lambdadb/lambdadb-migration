package cmd

import (
	"strings"
	"testing"
)

func TestOutputFormatForPath(t *testing.T) {
	tests := []struct {
		path string
		want outputFormat
	}{
		{path: "-", want: outputFormatJSON},
		{path: "inventory.json", want: outputFormatJSON},
		{path: "inventory.yaml", want: outputFormatYAML},
		{path: "inventory.yml", want: outputFormatYAML},
		{path: "INVENTORY.YAML", want: outputFormatYAML},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			if got := outputFormatForPath(tt.path); got != tt.want {
				t.Fatalf("outputFormatForPath() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMarshalOutputYAML(t *testing.T) {
	data, err := marshalOutput("inventory.yaml", map[string]any{
		"mapping": map[string]any{"target": map[string]any{"collection": "articles"}},
	})
	if err != nil {
		t.Fatalf("marshalOutput() error = %v", err)
	}
	if strings.Contains(string(data), `{"mapping"`) {
		t.Fatalf("marshalOutput() = %s, want YAML", data)
	}
	if !strings.Contains(string(data), "mapping:") {
		t.Fatalf("marshalOutput() = %s, want mapping key", data)
	}
}
