package elasticsearch

import (
	"fmt"

	"github.com/lambdadb/go-lambdadb/models/components"
	"github.com/lambdadb/lambdadb-migration/internal/source"
)

type settingsResponse map[string]struct {
	Settings struct {
		Index struct {
			Analysis struct {
				Analyzer map[string]map[string]any `json:"analyzer"`
			} `json:"analysis"`
		} `json:"index"`
	} `json:"settings"`
}

// Resolve only unmodified built-in presets, never a custom analysis pipeline.
func resolveAnalyzer(name string, definitions map[string]map[string]any) (string, error) {
	if definition, ok := definitions[name]; ok {
		typ, _ := definition["type"].(string)
		if len(definition) != 1 || typ == "" {
			return "", fmt.Errorf("analyzer %q has custom or configured options; choose a fixed LambdaDB preset explicitly", name)
		}
		name = typ
	} else if name == "default" {
		// Elasticsearch registers standard as default when no index definition exists.
		name = "standard"
	}
	analyzer := components.Analyzer(name)
	if !analyzer.IsExact() {
		return "", fmt.Errorf("unsupported source analyzer %q; choose a fixed LambdaDB preset explicitly", name)
	}
	switch name {
	case "nepali", "tamil", "telugu", "korean", "japanese", "chinese":
		return "", fmt.Errorf("source analyzer %q is not a shared built-in Elasticsearch/OpenSearch preset; plugin/custom analysis requires manual review", name)
	}
	return name, nil
}

func fieldAnalyzers(inv *source.Inventory, path string, field fieldMapping, definitions map[string]map[string]any) []string {
	if field.SearchAnalyzer != "" || field.SearchQuoteAnalyzer != "" {
		inv.Warnings = append(inv.Warnings, fmt.Sprintf("text field %q search_analyzer=%q and search_quote_analyzer=%q are not translated; review target query behavior", path, field.SearchAnalyzer, field.SearchQuoteAnalyzer))
	}
	if _, ok := definitions["default_search"]; ok {
		inv.Warnings = append(inv.Warnings, fmt.Sprintf("text field %q index default_search analyzer is not translated; review target query behavior", path))
	}
	name := field.Analyzer
	if name == "" {
		if _, ok := definitions["default"]; ok {
			name = "default"
		}
	}
	var analyzers []string
	if name != "" {
		preset, err := resolveAnalyzer(name, definitions)
		if err != nil {
			inv.Warnings = append(inv.Warnings, fmt.Sprintf("text field %q: %v; generated mapping is unsupported until explicitly edited", path, err))
			return []string{"unsupported:" + name}
		}
		analyzers = []string{preset}
	}
	return analyzers
}
