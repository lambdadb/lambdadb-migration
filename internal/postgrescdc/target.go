package postgrescdc

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"reflect"
	"regexp"
	"time"

	lambdadb "github.com/lambdadb/go-lambdadb"
	"github.com/lambdadb/go-lambdadb/models/apierrors"
	"github.com/lambdadb/go-lambdadb/models/components"
	"github.com/lambdadb/go-lambdadb/retry"
)

// Target must complete writes in order and fail on uncertain outcomes. A fresh,
// never-reused generation is mandatory after any process exit or apply failure.
type Target interface {
	Create(context.Context, Config) (string, error)
	Put(context.Context, string, string, Row) error
	Delete(context.Context, string, string) error
	Verify(context.Context, string, map[string]Row) error
}

type authorizedTarget struct {
	Target
	check func() error
}

func (t *authorizedTarget) Create(ctx context.Context, c Config) (string, error) {
	if err := t.check(); err != nil {
		return "", err
	}
	return t.Target.Create(ctx, c)
}
func (t *authorizedTarget) Put(ctx context.Context, g, id string, row Row) error {
	if err := t.check(); err != nil {
		return err
	}
	return t.Target.Put(ctx, g, id, row)
}
func (t *authorizedTarget) Delete(ctx context.Context, g, id string) error {
	if err := t.check(); err != nil {
		return err
	}
	return t.Target.Delete(ctx, g, id)
}
func (t *authorizedTarget) Verify(ctx context.Context, g string, rows map[string]Row) error {
	if err := t.check(); err != nil {
		return err
	}
	return t.Target.Verify(ctx, g, rows)
}

type LambdaTarget struct{ Client *lambdadb.Client }

func NewLambdaTarget(c Config, key string) *LambdaTarget {
	client := &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	configureProcessTestTLS(client)
	return &LambdaTarget{Client: lambdadb.New(lambdadb.WithBaseURL(c.TargetURL), lambdadb.WithProjectName(c.TargetProject), lambdadb.WithAPIKey(key), lambdadb.WithRetryConfig(retry.Config{Strategy: "none"}), lambdadb.WithClient(client))}
}

// Verify checks every committed document while application is paused at the
// source barrier. No counts, samples, or pending-inclusive reads establish this.
func (t *LambdaTarget) Verify(ctx context.Context, generation string, expected map[string]Row) error {
	ctx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	want := make(map[string]any, len(expected))
	for id, row := range expected {
		want[id] = document(id, row)
	}
	for {
		got := map[string]any{}
		seen := map[string]bool{}
		var token *string
		complete := false
		for {
			page, err := t.Client.Collection(generation).Docs().List(ctx, &lambdadb.ListDocsOpts{Size: lambdadb.Int64(100), PageToken: token, IncludeVectors: lambdadb.Bool(true)})
			if err != nil {
				break
			}
			for _, doc := range page.Docs {
				id, ok := doc.Doc["id"].(string)
				if !ok {
					return errors.New("target verification encountered an invalid ID")
				}
				if _, duplicate := got[id]; duplicate {
					return errors.New("target verification encountered duplicate IDs")
				}
				got[id] = doc.Doc
			}
			if len(got) > MaxRows {
				return errors.New("target verification exceeds row budget")
			}
			token = page.NextPageToken
			if token == nil || *token == "" {
				complete = true
				break
			}
			if seen[*token] {
				return errors.New("target verification pagination did not advance")
			}
			seen[*token] = true
		}
		if complete && equalJSON(want, got) {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.New("complete committed target verification failed")
		case <-time.After(500 * time.Millisecond):
		}
	}
}

type plannedTarget interface {
	PlanGeneration(Config) (string, error)
	CreatePlanned(context.Context, Config, string) error
}

var errGenerationCollision = errors.New("generation name collision; manual inspection required")

func (t *LambdaTarget) PlanGeneration(c Config) (string, error) {
	var suffix [12]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", err
	}
	return c.CollectionPrefix + "_" + hex.EncodeToString(suffix[:]), nil
}
func (t *LambdaTarget) CreatePlanned(ctx context.Context, c Config, collection string) error {
	var indexes map[string]components.IndexConfigsUnion
	if json.Unmarshal(c.IndexConfigs, &indexes) != nil {
		return errors.New("invalid LambdaDB index configuration")
	}
	_, err := t.Client.Collections.Create(ctx, lambdadb.CreateCollectionOptions{CollectionName: collection, IndexConfigs: indexes})
	var collision *apierrors.ResourceAlreadyExistsError
	if errors.As(err, &collision) {
		return errGenerationCollision
	}
	if err != nil {
		return errors.New("target generation creation outcome uncertain")
	}
	return nil
}
func (t *LambdaTarget) Create(ctx context.Context, c Config) (string, error) {
	name, err := t.PlanGeneration(c)
	if err != nil {
		return "", err
	}
	if err = t.CreatePlanned(ctx, c, name); err != nil {
		return "", err
	}
	return name, nil
}
func validGenerationName(c Config, name string) bool {
	return regexp.MustCompile("^" + regexp.QuoteMeta(c.CollectionPrefix) + "_[0-9a-f]{24}$").MatchString(name)
}
func (t *LambdaTarget) RemoveGeneration(ctx context.Context, c Config, name string) error {
	if !validGenerationName(c, name) {
		return errors.New("refusing to remove an invalid owned generation")
	}
	_, err := t.Client.Collection(name).Delete(ctx)
	var missing *apierrors.ResourceNotFoundError
	if err != nil && !errors.As(err, &missing) {
		return errors.New("generation deletion outcome uncertain")
	}
	// Acknowledgment alone does not prove logical unreachability. Physical storage
	// reclamation remains the data plane's retention/garbage-collection contract.
	for {
		_, err = t.Client.Collection(name).Get(ctx)
		if errors.As(err, &missing) {
			return nil
		}
		if err != nil {
			return errors.New("generation deletion could not be verified")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func document(id string, row Row) map[string]any {
	doc := make(map[string]any, len(row)+1)
	for k, v := range row {
		// The SDK's recursive marshaler treats RawMessage as a byte slice.
		// Decode into supported JSON values while retaining exact numbers.
		decoder := json.NewDecoder(bytes.NewReader(v))
		decoder.UseNumber()
		var value any
		_ = decoder.Decode(&value) // Row images contain validated JSON.
		doc[k] = value
	}
	doc["id"] = id
	return doc
}
func (t *LambdaTarget) Put(ctx context.Context, generation, id string, row Row) error {
	_, err := t.Client.Collection(generation).Docs().Upsert(ctx, lambdadb.UpsertDocsInput{Docs: []map[string]any{document(id, row)}})
	if err != nil {
		var bad *apierrors.BadRequestError
		if errors.As(err, &bad) {
			return fmt.Errorf("target upsert rejected (HTTP 400); source schema/mapping requires validation")
		}
		return fmt.Errorf("target upsert outcome uncertain (%T); stop and recover into a fresh generation", err)
	}
	return t.await(ctx, generation, id, document(id, row))
}
func (t *LambdaTarget) Delete(ctx context.Context, generation, id string) error {
	_, err := t.Client.Collection(generation).Docs().Delete(ctx, lambdadb.DeleteDocsInput{Ids: []string{id}})
	if err != nil {
		return errors.New("target delete outcome uncertain; stop and recover into a fresh generation")
	}
	return t.await(ctx, generation, id, nil)
}

// Verify pending-inclusive branch document reads after each durable API write.
// This is neither a committed-snapshot nor a BM25/vector searchable watermark.
func (t *LambdaTarget) await(ctx context.Context, generation, id string, expected map[string]any) error {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		result, err := t.Client.Collection(generation).Docs().Fetch(ctx, lambdadb.FetchDocsInput{Ids: []string{id}, ConsistentRead: lambdadb.Bool(true), IncludeVectors: lambdadb.Bool(true)})
		if err == nil {
			if expected == nil && len(result.Docs) == 0 {
				return nil
			}
			if expected != nil && len(result.Docs) == 1 && equalJSON(expected, result.Docs[0].Doc) {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return errors.New("target branch document visibility unverified; stop and recover into a fresh generation")
		case <-ticker.C:
		}
	}
}

func equalJSON(a, b any) bool {
	decode := func(v any) (any, error) {
		raw, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		d := json.NewDecoder(bytes.NewReader(raw))
		d.UseNumber()
		var out any
		err = d.Decode(&out)
		return out, err
	}
	x, err := decode(a)
	if err != nil {
		return false
	}
	y, err := decode(b)
	if err != nil {
		return false
	}
	return equalValue(x, y)
}
func equalValue(a, b any) bool {
	switch x := a.(type) {
	case json.Number:
		y, ok := b.(json.Number)
		if !ok {
			return false
		}
		r, ok := new(big.Rat).SetString(string(x))
		if !ok {
			return false
		}
		s, ok := new(big.Rat).SetString(string(y))
		return ok && r.Cmp(s) == 0
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for k, v := range x {
			w, ok := y[k]
			if !ok || !equalValue(v, w) {
				return false
			}
		}
		return true
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !equalValue(x[i], y[i]) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(a, b)
	}
}

// SearchToken reads committed search results; pending-inclusive document reads
// are deliberately not used as proof of this query's visibility.
func (t *LambdaTarget) SearchToken(ctx context.Context, generation, field, term string) ([]map[string]any, int64, error) {
	result, err := t.Client.Collection(generation).Query(ctx, lambdadb.QueryInput{Size: lambdadb.Int64(MaxServingRows), ConsistentRead: lambdadb.Bool(false), IncludeVectors: lambdadb.Bool(true), Query: map[string]any{"queryString": map[string]any{"query": term, "defaultField": field, "skipSyntax": true}}})
	if err != nil {
		return nil, 0, errors.New("target search failed")
	}
	docs := make([]map[string]any, 0, len(result.Docs))
	for _, hit := range result.Docs {
		docs = append(docs, hit.Doc)
	}
	return docs, result.Total, nil
}
