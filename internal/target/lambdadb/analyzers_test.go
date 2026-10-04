package lambdadb

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/lambdadb/lambdadb-migration/internal/config"
	"github.com/lambdadb/lambdadb-migration/internal/source"
	"gopkg.in/yaml.v3"
)

// Fixed fixture from server merge 55d888299fee44466326a9db8016af9811ade13b.
// Keep independent of SDK validation so missing enum values fail the wire test.
var analyzerPresets = strings.Fields(`standard english korean japanese arabic chinese cjk french german hindi indonesian italian portuguese russian spanish turkish armenian basque bengali brazilian bulgarian catalan czech danish dutch estonian finnish galician greek hungarian irish latvian lithuanian norwegian persian romanian serbian sorani swedish thai simple whitespace stop keyword pattern fingerprint nepali tamil telugu`)

func TestAnalyzerMappingToCollectionWire(t *testing.T) {
	if len(analyzerPresets) != 49 {
		t.Fatal("expected 49 fixed presets")
	}
	cases := []struct {
		name  string
		names []string
	}{
		{"omitted", nil}, {"empty", []string{}},
		{"order and duplicates", []string{"telugu", "keyword", "standard", "telugu"}},
		{"all presets", analyzerPresets},
	}
	for _, preset := range analyzerPresets {
		cases = append(cases, struct {
			name  string
			names []string
		}{preset, []string{preset}})
	}
	for _, tc := range cases {
		for _, format := range []string{"json", "yaml"} {
			t.Run(tc.name+"/"+format, func(t *testing.T) {
				inv := &source.Inventory{CollectionName: "source", PayloadIndexes: map[string]source.PayloadIndex{
					"metadata.title": {Name: "metadata.title", Type: "text", Analyzers: tc.names},
					"label":          {Name: "label", Type: "keyword"},
				}}
				mapping := config.MappingFromInventory(inv, "articles")
				var encoded []byte
				var err error
				if format == "json" {
					encoded, err = json.Marshal(mapping)
				} else {
					encoded, err = yaml.Marshal(mapping)
				}
				if err != nil {
					t.Fatal(err)
				}
				mapping, err = config.DecodeMapping(encoded)
				if err != nil {
					t.Fatal(err)
				}
				if err := config.ValidateMapping(inv, mapping, "articles", config.WriteModeBulk); err != nil {
					t.Fatal(err)
				}
				posts := 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					if r.Method == http.MethodGet && r.URL.Path == collectionPath {
						w.WriteHeader(404)
						fmt.Fprint(w, `{"message":"not found"}`)
						return
					}
					if r.Method != http.MethodPost || r.URL.Path != "/projects/test/collections" {
						t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
						w.WriteHeader(400)
						return
					}
					posts++
					var body struct {
						IndexConfigs map[string]json.RawMessage `json:"indexConfigs"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					want := map[string]any{"type": "text"}
					if tc.names != nil {
						want["analyzers"] = tc.names
					}
					wantJSON, _ := json.Marshal(want)
					var gotValue, wantValue any
					if err := json.Unmarshal(body.IndexConfigs["metadata_title"], &gotValue); err != nil {
						t.Error(err)
					}
					_ = json.Unmarshal(wantJSON, &wantValue)
					if !reflect.DeepEqual(gotValue, wantValue) {
						t.Errorf("wire=%s want=%s", body.IndexConfigs["metadata_title"], wantJSON)
					}
					if string(body.IndexConfigs["label"]) != `{"type":"keyword"}` {
						t.Errorf("keyword field changed: %s", body.IndexConfigs["label"])
					}
					w.WriteHeader(201)
					fmt.Fprint(w, createdCollectionJSON)
				}))
				defer server.Close()
				if err := newContractTarget(server.URL, config.WriteModeBulk).EnsureCollection(context.Background(), inv, mapping); err != nil {
					t.Fatal(err)
				}
				if posts != 1 {
					t.Fatalf("posts=%d", posts)
				}
			})
		}
	}
}

func TestAnalyzerValidationAndSchemaRejectUnsupportedSettings(t *testing.T) {
	for _, raw := range []string{
		`{"analyzers":["English"]}`, `{"analyzers":[" english "]}`, `{"analyzers":[""]}`,
		`{"analyzers":["unknown"]}`, `{"analyzers":["english",42]}`, `{"analyzers":"english"}`,
		`{"analyzers":[{"type":"english"}]}`, `{"analyzers":["english"],"stopwords":[]}`,
		`{"analyzer":"english"}`, `{"tokenizer":"standard"}`, `{"filter":["lowercase"]}`,
	} {
		t.Run(raw, func(t *testing.T) {
			var index map[string]any
			if err := json.Unmarshal([]byte(raw), &index); err != nil {
				t.Fatal(err)
			}
			index["type"] = "text"
			inv := &source.Inventory{CollectionName: "source"}
			mapping := config.MappingFromInventory(inv, "articles")
			mapping.Payload.IndexConfigs["title"] = index
			if err := config.ValidateMapping(inv, mapping, "articles", config.WriteModeBulk); err == nil {
				t.Fatal("validation accepted unsupported settings")
			}
			if _, err := buildIndexConfigs(mapping); err == nil {
				t.Fatal("schema silently discarded unsupported settings")
			}
		})
	}
	for _, value := range []any{nil, []string(nil), []any(nil)} {
		cfg, err := buildPayloadIndexConfig(map[string]any{"type": "text", "analyzers": value})
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		want := `{"analyzers":[],"type":"text"}`
		if value == nil {
			want = `{"type":"text"}`
		}
		if string(raw) != want {
			t.Fatalf("wire=%s want=%s", raw, want)
		}
	}
}

func TestSDKUpgradeVerificationSearchWire(t *testing.T) {
	for _, sparse := range []bool{false, true} {
		t.Run(fmt.Sprintf("sparse=%t", sparse), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]json.RawMessage
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				if r.Method != "POST" || r.URL.Path != collectionPath+"/query" {
					t.Errorf("unexpected query route")
				}
				if _, ok := body["rerank"]; ok {
					t.Error("migration verification must not rerank")
				}
				if string(body["size"]) != "2" || string(body["consistentRead"]) != "true" {
					t.Errorf("changed query defaults: %v", body)
				}
				want := `{"knn":{"field":"vector","queryVector":[0.25,0.5],"k":2}}`
				if sparse {
					want = `{"sparseVector":{"field":"vector","queryVector":{"1":0.25,"7":0.5}}}`
				}
				var gotQuery, wantQuery any
				_ = json.Unmarshal(body["query"], &gotQuery)
				_ = json.Unmarshal([]byte(want), &wantQuery)
				if !reflect.DeepEqual(gotQuery, wantQuery) {
					t.Errorf("query=%s want=%s", body["query"], want)
				}
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"isDocsInline":true,"docs":[{"collection":"articles","score":2,"doc":{"id":"second"}},{"collection":"articles","score":1,"doc":{"id":"first"}}],"total":2,"took":1}`)
			}))
			defer server.Close()
			var ids []string
			var err error
			target := newContractTarget(server.URL, config.WriteModeBulk)
			if sparse {
				ids, err = target.QuerySparse(context.Background(), "id", "vector", map[string]float32{"1": 0.25, "7": 0.5}, 2)
			} else {
				ids, err = target.QueryKNN(context.Background(), "id", "vector", []float32{0.25, 0.5}, 2)
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(ids, []string{"second", "first"}) {
				t.Fatalf("server order changed: %v", ids)
			}
		})
	}
}
