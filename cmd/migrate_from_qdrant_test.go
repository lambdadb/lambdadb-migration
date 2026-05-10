package cmd

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
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

	report, err := validateMigration(context.Background(), target, 1, 1, []map[string]any{sample}, config.MappingConfig{})
	if err != nil {
		t.Fatalf("validateMigration() error = %v", err)
	}
	if report.Status != "pass" || report.Samples.Compared != 1 {
		t.Fatalf("validation report = %#v, want pass with one compared sample", report)
	}
}

func TestValidateMigrationRejectsAcceptedCountMismatch(t *testing.T) {
	report, err := validateMigration(context.Background(), fakeValidationTarget{}, 2, 1, nil, config.MappingConfig{})
	if err == nil || !strings.Contains(err.Error(), "accepted 1 records") {
		t.Fatalf("validateMigration() error = %v, want accepted count mismatch", err)
	}
	if report.Status != "fail" || len(report.Errors) == 0 {
		t.Fatalf("validation report = %#v, want failed report with errors", report)
	}
}

func TestValidateMigrationRejectsSampleMismatch(t *testing.T) {
	target := fakeValidationTarget{
		count: 1,
		docs:  []map[string]any{{"id": "1", "title": "actual"}},
	}
	report, err := validateMigration(context.Background(), target, 1, 1, []map[string]any{{"id": "1", "title": "expected"}}, config.MappingConfig{})
	if err == nil || !strings.Contains(err.Error(), "field \"title\" mismatch") {
		t.Fatalf("validateMigration() error = %v, want sample mismatch", err)
	}
	if report.Samples.Fetched != 1 || report.Samples.Compared != 0 {
		t.Fatalf("validation report samples = %#v, want fetched but not compared", report.Samples)
	}
}

func TestWriteValidationReport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reports", "validation.json")
	report := validationReport{
		Status:          "pass",
		SourceCount:     1,
		AcceptedRecords: 1,
		Samples: validationSampleReport{
			Requested: 1,
			Fetched:   1,
			Compared:  1,
			IDs:       []string{"1"},
		},
	}

	if err := writeValidationReport(path, report); err != nil {
		t.Fatalf("writeValidationReport() error = %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read validation report: %v", err)
	}
	var got validationReport
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("decode validation report: %v", err)
	}
	if got.Status != "pass" || got.Samples.IDs[0] != "1" {
		t.Fatalf("validation report = %#v, want written report", got)
	}
}
