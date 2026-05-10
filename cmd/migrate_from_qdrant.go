package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/lambdadb/lambdadb-migration/internal/checkpoint"
	"github.com/lambdadb/lambdadb-migration/internal/config"
	"github.com/lambdadb/lambdadb-migration/internal/source"
	qdrantsource "github.com/lambdadb/lambdadb-migration/internal/source/qdrant"
	targetlambdadb "github.com/lambdadb/lambdadb-migration/internal/target/lambdadb"
	"github.com/lambdadb/lambdadb-migration/internal/transform"
)

type MigrateQdrantCmd struct {
	Qdrant      config.QdrantConfig    `embed:"" prefix:"qdrant."`
	LambdaDB    config.LambdaDBConfig  `embed:"" prefix:"lambdadb."`
	Migration   config.MigrationConfig `embed:"" prefix:"migration."`
	MappingFile string                 `help:"Path to a JSON or YAML migration mapping file."`
}

const (
	validationPollInterval = 1 * time.Second
	validationTimeout      = 5 * time.Minute
)

func (c *MigrateQdrantCmd) Run(globals *Globals) error {
	if err := c.Migration.ValidateConfig(); err != nil {
		return err
	}

	ctx := context.Background()
	src, err := qdrantsource.New(c.Qdrant)
	if err != nil {
		return err
	}
	defer src.Close()

	inv, err := src.Inventory(ctx)
	if err != nil {
		return err
	}

	mapping, err := c.loadMapping(inv)
	if err != nil {
		return err
	}
	if err := config.ValidateMapping(inv, mapping, c.LambdaDB.Collection, c.Migration.WriteMode); err != nil {
		return err
	}
	if c.Migration.DryRun {
		return printDryRun(inv.RecordCount, mapping)
	}

	target := targetlambdadb.New(c.LambdaDB, c.Migration.WriteMode, targetlambdadb.WriteRetryPolicy{
		MaxAttempts:  c.Migration.RetryMaxAttempts,
		InitialDelay: time.Duration(c.Migration.RetryInitialDelayMS) * time.Millisecond,
		MaxDelay:     time.Duration(c.Migration.RetryMaxDelayMS) * time.Millisecond,
	})
	if err := target.EnsureCollection(ctx, inv, mapping); err != nil {
		return err
	}
	maxBatchBytes := targetlambdadb.EffectiveMaxBatchBytes(c.Migration.MaxBatchBytes, c.Migration.WriteMode)

	store := checkpoint.NewFileStore(checkpointRoot(c.Migration.CheckpointPath))
	key := checkpointKey(c.Qdrant.Collection, c.LambdaDB.ProjectName, c.LambdaDB.Collection)
	var cursorValue any
	var accepted uint64
	sampleLimit := c.Migration.ValidationSampleSize
	samples := make([]map[string]any, 0, sampleLimit)
	startedAt := time.Now()
	if !c.Migration.Restart {
		cp, err := store.Load(ctx, key)
		if err != nil {
			return err
		}
		if cp != nil {
			cursorValue = cp.Cursor
			accepted = cp.AcceptedRecords
			fmt.Fprintf(os.Stderr, "resuming from checkpoint: acceptedRecords=%d\n", accepted)
		}
	}
	progress := newProgressTracker(inv.RecordCount, accepted, startedAt)

	for {
		batch, err := src.Read(ctx, source.Cursor{Value: cursorValue}, c.Migration.BatchSize)
		if err != nil {
			return err
		}
		if len(batch.Records) == 0 && batch.Done {
			break
		}

		docs := make([]map[string]any, 0, len(batch.Records))
		for _, record := range batch.Records {
			doc, err := transform.RecordToDocumentWithMapping(record, mapping)
			if err != nil {
				return err
			}
			docs = append(docs, doc)
			if c.Migration.Validate && len(samples) < sampleLimit {
				samples = append(samples, cloneDocument(doc))
			}
		}
		writeBatches, err := targetlambdadb.SplitDocumentsByPayloadSize(docs, maxBatchBytes)
		if err != nil {
			return err
		}
		for _, writeBatch := range writeBatches {
			if err := target.Write(ctx, writeBatch); err != nil {
				return err
			}
		}

		accepted += uint64(len(docs))
		if batch.NextCursor != nil {
			cursorValue = batch.NextCursor.Value
		}
		if err := store.Save(ctx, key, checkpoint.Checkpoint{
			SourceKind:       "qdrant",
			SourceCollection: c.Qdrant.Collection,
			TargetCollection: c.LambdaDB.Collection,
			Cursor:           cursorValue,
			AcceptedRecords:  accepted,
			UpdatedAt:        time.Now().UTC(),
		}); err != nil {
			return err
		}

		fmt.Fprintln(os.Stderr, progress.BatchLine(accepted, len(docs), time.Now()))
		if batch.Done {
			break
		}
		if c.Migration.BatchDelayMS > 0 {
			time.Sleep(time.Duration(c.Migration.BatchDelayMS) * time.Millisecond)
		}
	}

	fmt.Fprintln(os.Stderr, progress.CompleteLine(accepted, c.LambdaDB.Collection, time.Now()))
	if c.Migration.Validate {
		if err := validateMigration(ctx, target, inv.RecordCount, accepted, samples, mapping); err != nil {
			return err
		}
	}
	if c.Migration.CleanupCheckpoint {
		if err := store.Delete(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

type validationTarget interface {
	Count(context.Context) (uint64, error)
	Fetch(context.Context, []string) ([]map[string]any, error)
}

func validateMigration(ctx context.Context, target validationTarget, sourceCount, accepted uint64, samples []map[string]any, mapping config.MappingConfig) error {
	var errs []error
	if accepted != sourceCount {
		errs = append(errs, fmt.Errorf("accepted %d records but source inventory reported %d", accepted, sourceCount))
	}

	if count, err := target.Count(ctx); err != nil {
		errs = append(errs, fmt.Errorf("read LambdaDB collection count: %w", err))
	} else {
		fmt.Fprintf(os.Stderr, "validation LambdaDB numDocs=%d accepted=%d source=%d\n", count, accepted, sourceCount)
	}

	if len(samples) == 0 {
		if accepted > 0 {
			fmt.Fprintln(os.Stderr, "validation sample fetch skipped")
		}
		return errors.Join(errs...)
	}

	idField := mapping.IDs.TargetField
	if idField == "" {
		idField = "id"
	}
	ids := make([]string, 0, len(samples))
	expectedByID := map[string]map[string]any{}
	for _, sample := range samples {
		id, ok := sample[idField].(string)
		if !ok || id == "" {
			errs = append(errs, fmt.Errorf("sample document has non-string id field %q: %#v", idField, sample[idField]))
			continue
		}
		ids = append(ids, id)
		expectedByID[id] = sample
	}
	if len(ids) == 0 {
		return errors.Join(errs...)
	}

	docs, err := waitForFetchedDocuments(ctx, target, ids)
	if err != nil {
		errs = append(errs, err)
		return errors.Join(errs...)
	}
	actualByID := map[string]map[string]any{}
	for _, doc := range docs {
		id, ok := doc[idField].(string)
		if ok {
			actualByID[id] = doc
		}
	}
	for _, id := range ids {
		expected := expectedByID[id]
		actual, ok := actualByID[id]
		if !ok {
			errs = append(errs, fmt.Errorf("sample document %q was not returned by LambdaDB fetch", id))
			continue
		}
		if err := compareSampleDocument(id, expected, actual); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		fmt.Fprintf(os.Stderr, "validation fetched and compared %d sample documents\n", len(ids))
	}
	return errors.Join(errs...)
}

func waitForFetchedDocuments(ctx context.Context, target validationTarget, ids []string) ([]map[string]any, error) {
	waitCtx, cancel := context.WithTimeout(ctx, validationTimeout)
	defer cancel()

	ticker := time.NewTicker(validationPollInterval)
	defer ticker.Stop()

	var lastCount int
	for {
		docs, err := target.Fetch(waitCtx, ids)
		if err == nil {
			lastCount = len(docs)
			if len(docs) >= len(ids) {
				return docs, nil
			}
		}

		select {
		case <-waitCtx.Done():
			if err != nil {
				return nil, fmt.Errorf("fetch validation samples from LambdaDB: last error: %w", err)
			}
			return nil, fmt.Errorf("fetch validation samples from LambdaDB: got %d/%d before timeout: %w", lastCount, len(ids), waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func compareSampleDocument(id string, expected, actual map[string]any) error {
	for field, expectedValue := range expected {
		actualValue, ok := actual[field]
		if !ok {
			return fmt.Errorf("sample document %q missing field %q", id, field)
		}
		if !valuesEqual(expectedValue, actualValue) {
			return fmt.Errorf("sample document %q field %q mismatch: expected %#v got %#v", id, field, expectedValue, actualValue)
		}
	}
	return nil
}

func valuesEqual(expected, actual any) bool {
	if reflect.DeepEqual(expected, actual) {
		return true
	}
	switch e := expected.(type) {
	case []float32:
		a, ok := actual.([]any)
		if !ok || len(e) != len(a) {
			return false
		}
		for i, ev := range e {
			if !numericEqual(float64(ev), a[i]) {
				return false
			}
		}
		return true
	case map[string]float32:
		a, ok := actual.(map[string]any)
		if !ok || len(e) != len(a) {
			return false
		}
		for key, ev := range e {
			if !numericEqual(float64(ev), a[key]) {
				return false
			}
		}
		return true
	case map[string]any:
		a, ok := actual.(map[string]any)
		if !ok || len(e) != len(a) {
			return false
		}
		for key, ev := range e {
			if !valuesEqual(ev, a[key]) {
				return false
			}
		}
		return true
	case []any:
		a, ok := actual.([]any)
		if !ok || len(e) != len(a) {
			return false
		}
		for i, ev := range e {
			if !valuesEqual(ev, a[i]) {
				return false
			}
		}
		return true
	default:
		return numericEqual(expected, actual)
	}
}

func numericEqual(expected, actual any) bool {
	ef, eok := asFloat64(expected)
	af, aok := asFloat64(actual)
	if !eok || !aok {
		return false
	}
	const epsilon = 0.000001
	delta := ef - af
	return delta >= -epsilon && delta <= epsilon
}

func asFloat64(value any) (float64, bool) {
	switch v := value.(type) {
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint64:
		return float64(v), true
	case json.Number:
		f, err := v.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

func cloneDocument(doc map[string]any) map[string]any {
	out := make(map[string]any, len(doc))
	for key, value := range doc {
		out[key] = value
	}
	return out
}

func (c *MigrateQdrantCmd) loadMapping(inv *source.Inventory) (config.MappingConfig, error) {
	if c.MappingFile == "" {
		return config.MappingFromInventory(inv, c.LambdaDB.Collection), nil
	}
	data, err := os.ReadFile(c.MappingFile)
	if err != nil {
		return config.MappingConfig{}, fmt.Errorf("read mapping file: %w", err)
	}
	mapping, err := config.DecodeMapping(data)
	if err != nil {
		return config.MappingConfig{}, fmt.Errorf("decode mapping file: %w", err)
	}
	return mapping, nil
}

func printDryRun(recordCount uint64, mapping config.MappingConfig) error {
	data, err := json.MarshalIndent(struct {
		RecordCount uint64               `json:"recordCount"`
		Mapping     config.MappingConfig `json:"mapping"`
	}{RecordCount: recordCount, Mapping: mapping}, "", "  ")
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(append(data, '\n'))
	return err
}

func checkpointRoot(path string) string {
	if path != "" {
		return path
	}
	return filepath.Join(".lambdadb-migration", "checkpoints")
}

func checkpointKey(sourceCollection, targetProject, targetCollection string) string {
	parts := []string{"qdrant", sourceCollection, targetProject, targetCollection}
	for i, part := range parts {
		parts[i] = strings.NewReplacer("/", "_", "\\", "_", ":", "_").Replace(part)
	}
	return filepath.Join(parts...)
}
