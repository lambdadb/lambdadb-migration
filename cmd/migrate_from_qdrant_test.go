package cmd

import (
	"context"
	"strings"
	"testing"

	"github.com/lambdadb/lambdadb-migration/internal/config"
)

type fakeValidationTarget struct {
	count uint64
	docs  []map[string]any
}

func (f fakeValidationTarget) Count(context.Context) (uint64, error) {
	return f.count, nil
}

func (f fakeValidationTarget) Fetch(context.Context, []string) ([]map[string]any, error) {
	return f.docs, nil
}

func TestValidateMigrationComparesSamples(t *testing.T) {
	sample := map[string]any{
		"id":       "1",
		"dense":    []float32{0.1, 0.2},
		"metadata": map[string]float32{"3": 0.7},
	}
	target := fakeValidationTarget{
		count: 1,
		docs: []map[string]any{{
			"id":       "1",
			"dense":    []any{0.1, 0.2},
			"metadata": map[string]any{"3": 0.7},
		}},
	}

	if err := validateMigration(context.Background(), target, 1, 1, []map[string]any{sample}, config.MappingConfig{}); err != nil {
		t.Fatalf("validateMigration() error = %v", err)
	}
}

func TestValidateMigrationRejectsAcceptedCountMismatch(t *testing.T) {
	err := validateMigration(context.Background(), fakeValidationTarget{}, 2, 1, nil, config.MappingConfig{})
	if err == nil || !strings.Contains(err.Error(), "accepted 1 records") {
		t.Fatalf("validateMigration() error = %v, want accepted count mismatch", err)
	}
}

func TestValidateMigrationRejectsSampleMismatch(t *testing.T) {
	target := fakeValidationTarget{
		count: 1,
		docs:  []map[string]any{{"id": "1", "title": "actual"}},
	}
	err := validateMigration(context.Background(), target, 1, 1, []map[string]any{{"id": "1", "title": "expected"}}, config.MappingConfig{})
	if err == nil || !strings.Contains(err.Error(), "field \"title\" mismatch") {
		t.Fatalf("validateMigration() error = %v, want sample mismatch", err)
	}
}
