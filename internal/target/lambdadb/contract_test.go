package lambdadb

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/lambdadb/lambdadb-migration/internal/config"
)

const collectionPath = "/projects/test/collections/articles"

// These fixtures follow the public create/describe DTOs: both are wrapped,
// neither has collectionStatus, and creation returns only a metadata subset.
const createdCollectionJSON = `{"collection":{"collectionName":"articles","description":"","tags":{},"defaultBranchName":"main","snapshotRetentionInDays":7,"createdAt":1789516800000}}`
const describedCollectionJSON = `{"collection":{"projectName":"test","collectionName":"articles","indexConfigs":{},"description":"","tags":{},"numPartitions":1,"numDocs":42,"defaultBranchName":"main","snapshotRetentionInDays":7,"createdAt":1789516800000,"updatedAt":1789516800000}}`

func TestEnsureCollectionContract(t *testing.T) {
	for _, tt := range []struct {
		name       string
		skipCreate bool
		getStatus  int
		postStatus int
		wantCalls  []string
		wantErr    string
	}{
		{"existing", false, 200, 0, []string{"GET"}, ""},
		{"create_201", false, 404, 201, []string{"GET", "POST"}, ""},
		{"disabled", true, 0, 0, nil, ""},
		{"get_forbidden", false, 403, 0, []string{"GET"}, "get LambdaDB collection"},
		{"create_bad_request", false, 404, 400, []string{"GET", "POST"}, "create LambdaDB collection"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var mu sync.Mutex
			var calls []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				calls = append(calls, r.Method)
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == "GET" && r.URL.Path == collectionPath:
					w.WriteHeader(tt.getStatus)
					if tt.getStatus == 200 {
						fmt.Fprint(w, describedCollectionJSON)
					} else {
						fmt.Fprint(w, `{"message":"unavailable"}`)
					}
				case r.Method == "POST" && r.URL.Path == "/projects/test/collections":
					var body struct {
						CollectionName string         `json:"collectionName"`
						IndexConfigs   map[string]any `json:"indexConfigs"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.CollectionName != "articles" || body.IndexConfigs == nil {
						t.Errorf("create body = %+v, error = %v", body, err)
					}
					w.WriteHeader(tt.postStatus)
					if tt.postStatus == 201 {
						fmt.Fprint(w, createdCollectionJSON)
					} else {
						fmt.Fprint(w, `{"message":"invalid schema"}`)
					}
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			target := newContractTarget(server.URL, config.WriteModeUpsert)
			err := target.EnsureCollection(context.Background(), nil, config.MappingConfig{
				Target: config.MappingTarget{CreateCollection: !tt.skipCreate},
			})
			if (tt.wantErr == "" && err != nil) || (tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr))) {
				t.Fatalf("EnsureCollection() error = %v, want %q", err, tt.wantErr)
			}
			mu.Lock()
			defer mu.Unlock()
			if !reflect.DeepEqual(calls, tt.wantCalls) {
				t.Fatalf("requests = %v, want %v (no status polling)", calls, tt.wantCalls)
			}
		})
	}
}

func TestCountUsesWrappedCollectionResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != collectionPath {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, describedCollectionJSON)
	}))
	defer server.Close()
	count, err := newContractTarget(server.URL, config.WriteModeUpsert).Count(context.Background())
	if err != nil || count != 42 {
		t.Fatalf("Count() = %d, %v; want 42, nil", count, err)
	}
}

func TestBulkWriteContract(t *testing.T) {
	for _, tt := range []struct {
		name              string
		omitType          bool
		uploadFailure     int
		completionFailure int
		wantUploads       int
		wantCompletions   int
		wantErr           bool
	}{
		{name: "signed_headers", wantUploads: 1, wantCompletions: 1},
		{name: "default_json_type", omitType: true, wantUploads: 1, wantCompletions: 1},
		{name: "retry_upload_with_fresh_url", uploadFailure: 503, wantUploads: 2, wantCompletions: 1},
		{name: "retry_completion_without_reupload", completionFailure: 503, wantUploads: 1, wantCompletions: 2},
		{name: "create_only_conflict", uploadFailure: 412, wantUploads: 1, wantErr: true},
		{name: "completion_rejected", completionFailure: 400, wantUploads: 1, wantCompletions: 1, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var mu sync.Mutex
			infoCalls, uploads, completions := 0, 0, 0
			docs := []map[string]any{{"id": "one", "text": "hello"}}
			storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				uploads++
				if r.Method != "PUT" || r.URL.Path != fmt.Sprintf("/object-%d", uploads) {
					t.Errorf("upload reused a URL or used wrong method: %s %s", r.Method, r.URL.Path)
				}
				for key, want := range map[string]string{"Content-Type": "application/json", "If-None-Match": "*", "X-Amz-Meta-Upload": "signed-value"} {
					if got := r.Header.Get(key); got != want {
						t.Errorf("upload %s = %q, want %q", key, got, want)
					}
				}
				if r.Header.Get("Authorization") != "" || r.Header.Get("X-API-Key") != "" {
					t.Error("API credentials sent to upload storage")
				}
				var body struct {
					Docs []map[string]any `json:"docs"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil || !reflect.DeepEqual(body.Docs, docs) {
					t.Errorf("upload body = %+v, error = %v", body, err)
				}
				if uploads == 1 && tt.uploadFailure != 0 {
					w.WriteHeader(tt.uploadFailure)
					fmt.Fprint(w, "<Error><Code>UploadRejected</Code></Error>")
				}
			}))
			defer storage.Close()
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path != collectionPath+"/docs/bulk-upsert" {
					t.Errorf("unexpected API request: %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
					return
				}
				switch r.Method {
				case "GET":
					infoCalls++
					info := map[string]any{
						"url":        fmt.Sprintf("%s/object-%d", storage.URL, infoCalls),
						"objectKey":  fmt.Sprintf("object-%d", infoCalls),
						"httpMethod": "PUT", "sizeLimitBytes": 200000000,
						"headers": map[string]string{"if-none-match": "*", "x-amz-meta-upload": "signed-value"},
					}
					if !tt.omitType {
						info["type"] = "application/json"
					}
					json.NewEncoder(w).Encode(info)
				case "POST":
					completions++
					var body map[string]any
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Errorf("decode completion: %v", err)
					}
					if body["objectKey"] != fmt.Sprintf("object-%d", infoCalls) || body["type"] != "application/json" {
						t.Errorf("completion body = %v", body)
					}
					if completions == 1 && tt.completionFailure != 0 {
						w.WriteHeader(tt.completionFailure)
						fmt.Fprint(w, `{"message":"rejected"}`)
						return
					}
					w.WriteHeader(http.StatusAccepted)
					fmt.Fprint(w, `{"message":"accepted"}`)
				default:
					t.Errorf("unexpected method: %s", r.Method)
				}
			}))
			defer api.Close()
			err := newContractTarget(api.URL, config.WriteModeBulk).Write(context.Background(), docs)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Write() error = %v, wantErr %v", err, tt.wantErr)
			}
			mu.Lock()
			defer mu.Unlock()
			if infoCalls != tt.wantUploads || uploads != tt.wantUploads || completions != tt.wantCompletions {
				t.Fatalf("info/upload/complete = %d/%d/%d, want %d/%d/%d", infoCalls, uploads, completions, tt.wantUploads, tt.wantUploads, tt.wantCompletions)
			}
		})
	}
}

func newContractTarget(url string, mode config.WriteMode) *Target {
	return New(config.LambdaDBConfig{BaseURL: url, ProjectName: "test", Collection: "articles", APIKey: "test-key"}, mode, WriteRetryPolicy{MaxAttempts: 2})
}
