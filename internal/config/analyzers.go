package config

import (
	"fmt"
	"sort"

	"github.com/lambdadb/go-lambdadb/models/components"
)

// ParseTextIndexAnalyzers validates fixed presets against the pinned SDK enum.
// Nil uses the server default; an explicit empty array stays empty. Names are exact.
func ParseTextIndexAnalyzers(index map[string]any) ([]components.Analyzer, error) {
	var unsupported []string
	for key := range index {
		if key != "type" && key != "analyzers" {
			unsupported = append(unsupported, key)
		}
	}
	if len(unsupported) > 0 {
		sort.Strings(unsupported)
		return nil, fmt.Errorf("unsupported text index options %v; only fixed analyzer presets are supported", unsupported)
	}
	value := index["analyzers"]
	if value == nil {
		return nil, nil
	}
	var names []string
	switch value := value.(type) {
	case []string:
		names = value
	case []any:
		for _, item := range value {
			name, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("non-string analyzer %v", item)
			}
			names = append(names, name)
		}
	default:
		return nil, fmt.Errorf("analyzers must be a string array")
	}
	analyzers := make([]components.Analyzer, 0, len(names))
	for _, name := range names {
		analyzer := components.Analyzer(name)
		if !analyzer.IsExact() {
			return nil, fmt.Errorf("unsupported analyzer %q", name)
		}
		analyzers = append(analyzers, analyzer)
	}
	return analyzers, nil
}
