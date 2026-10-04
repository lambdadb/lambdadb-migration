package elasticsearch

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/lambdadb/lambdadb-migration/internal/config"
)

func TestInventoryPreservesAnalyzerSettings(t *testing.T) {
	for _, tc := range []struct {
		name, field, definitions, want, wantErr string
		warnings                                bool
	}{
		{"omitted", `{}`, `{}`, "", "", false},
		{"explicit implicit default", `{"analyzer":"default"}`, `{}`, "standard", "", false},
		{"explicit configured default", `{"analyzer":"default"}`, `{"default":{"type":"french"}}`, "french", "", false},
		{"explicit custom default", `{"analyzer":"default"}`, `{"default":{"type":"standard","max_token_length":"10"}}`, "", "custom or configured options", false},
		{"explicit", `{"analyzer":"german"}`, `{}`, "german", "", false},
		{"keyword analyzer", `{"analyzer":"keyword"}`, `{}`, "keyword", "", false},
		{"index default", `{}`, `{"default":{"type":"french"}}`, "french", "", false},
		{"override default", `{"analyzer":"english"}`, `{"default":{"type":"french"}}`, "english", "", false},
		{"preset alias", `{"analyzer":"plain"}`, `{"plain":{"type":"simple"}}`, "simple", "", false},
		{"search options", `{"analyzer":"english","search_analyzer":"simple","search_quote_analyzer":"whitespace"}`, `{"default_search":{"type":"stop"}}`, "english", "", true},
		{"custom", `{"analyzer":"custom_text","search_analyzer":"simple"}`, `{"custom_text":{"type":"custom","tokenizer":"standard"}}`, "", "custom or configured options", false},
		{"configured builtin", `{"analyzer":"english"}`, `{"english":{"type":"english","stopwords":"_none_"}}`, "", "custom or configured options", false},
		{"configured default", `{}`, `{"default":{"type":"standard","max_token_length":"10"}}`, "", "custom or configured options", false},
		{"unknown", `{"analyzer":"unknown"}`, `{}`, "", "unsupported source analyzer", false},
		{"case", `{"analyzer":"English"}`, `{}`, "", "unsupported source analyzer", false},
		{"lucene extension", `{"analyzer":"nepali"}`, `{}`, "", "not a shared built-in", false},
		{"plugin", `{"analyzer":"nori"}`, `{}`, "", "unsupported source analyzer", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var field map[string]any
			if err := json.Unmarshal([]byte(tc.field), &field); err != nil {
				t.Fatal(err)
			}
			field["type"] = "text"
			server := newTestServer(t, map[string]handlerFunc{
				"GET /articles/_mapping": func(t *testing.T, w responseWriter, r request) {
					writeJSON(t, w, map[string]any{"articles": map[string]any{"mappings": map[string]any{"properties": map[string]any{"metadata": map[string]any{"properties": map[string]any{"title": field}}}}}})
				},
				"GET /articles/_settings": func(t *testing.T, w responseWriter, r request) {
					var definitions map[string]any
					if err := json.Unmarshal([]byte(tc.definitions), &definitions); err != nil {
						t.Fatal(err)
					}
					writeJSON(t, w, map[string]any{"articles": map[string]any{"settings": map[string]any{"index": map[string]any{"analysis": map[string]any{"analyzer": definitions}}}}})
				},
				"GET /articles/_count": func(t *testing.T, w responseWriter, r request) { writeJSON(t, w, map[string]any{"count": 0}) },
			})
			src, err := New(config.ElasticsearchConfig{URL: server.URL, Index: "articles"})
			if err != nil {
				t.Fatal(err)
			}
			inv, err := src.Inventory(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			mapping := config.MappingFromInventory(inv, "articles")
			if tc.wantErr != "" {
				if field["search_analyzer"] != nil && !strings.Contains(strings.Join(inv.Warnings, "\n"), "search_analyzer") {
					t.Fatal("unsupported index analysis hid search analyzer warning")
				}
				if !strings.Contains(strings.Join(inv.Warnings, "\n"), tc.wantErr) {
					t.Fatalf("missing warning %q: %v", tc.wantErr, inv.Warnings)
				}
				if err := config.ValidateMapping(inv, mapping, "articles", config.WriteModeBulk); err == nil {
					t.Fatal("generated mapping silently accepted unsupported analysis")
				}
				mapping.Payload.IndexConfigs["metadata_title"] = map[string]any{"type": "text", "analyzers": []string{"standard"}}
				if err := config.ValidateMapping(inv, mapping, "articles", config.WriteModeBulk); err != nil {
					t.Fatalf("explicit manual mapping rejected: %v", err)
				}
				return
			}

			got := mapping.Payload.IndexConfigs["metadata_title"]["analyzers"]
			if tc.want == "" {
				if got != nil {
					t.Fatalf("default was rewritten: %v", got)
				}
			} else if !reflect.DeepEqual(got, []string{tc.want}) {
				t.Fatalf("analyzers=%v want=%s", got, tc.want)
			}
			if err := config.ValidateMapping(inv, mapping, "articles", config.WriteModeBulk); err != nil {
				t.Fatal(err)
			}
			if tc.warnings && !strings.Contains(strings.Join(inv.Warnings, "\n"), "not translated") {
				t.Fatal("search settings disappeared without warning")
			}
			if tc.want != "" {
				inv.PayloadIndexes["metadata.title"].Analyzers[0] = "modified"
				again, err := src.Inventory(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if again.PayloadIndexes["metadata.title"].Analyzers[0] != tc.want {
					t.Fatal("caller mutated cached analyzers")
				}
				if !reflect.DeepEqual(got, []string{tc.want}) {
					t.Fatal("inventory mutation changed mapping")
				}
			}
		})
	}
}

func TestInventoryCannotAssumeDefaultsWhenSettingsAreUnavailable(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"forbidden", 403, `{"error":"forbidden"}`},
		{"missing index", 200, `{}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := newTestServer(t, map[string]handlerFunc{
				"GET /articles/_mapping": func(t *testing.T, w responseWriter, r request) {
					writeJSON(t, w, map[string]any{"articles": map[string]any{"mappings": map[string]any{"properties": map[string]any{"title": map[string]any{"type": "text"}}}}})
				},
				"GET /articles/_count": func(t *testing.T, w responseWriter, r request) { writeJSON(t, w, map[string]any{"count": 0}) },
				"GET /articles/_settings": func(t *testing.T, w responseWriter, r request) {
					w.WriteHeader(tc.status)
					_, _ = w.Write([]byte(tc.body))
				},
			})
			src, err := New(config.ElasticsearchConfig{URL: server.URL, Index: "articles"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := src.Inventory(context.Background()); err == nil {
				t.Fatal("unreadable settings silently became standard")
			}
		})
	}
}
