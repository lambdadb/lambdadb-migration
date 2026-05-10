package cmd

import (
	"strings"
	"testing"
	"time"
)

func TestProgressBatchLine(t *testing.T) {
	start := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	tracker := newProgressTracker(10, 2, start)

	got := tracker.BatchLine(6, 4, start.Add(2*time.Second))
	for _, want := range []string{
		"accepted=6/10",
		"(60.0%)",
		"batch=4",
		"rate=2.0 records/s",
		"elapsed=2s",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("BatchLine() = %q, want containing %q", got, want)
		}
	}
}

func TestProgressCompleteLine(t *testing.T) {
	start := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	tracker := newProgressTracker(10, 0, start)

	got := tracker.CompleteLine(10, "articles", start.Add(5*time.Second))
	for _, want := range []string{
		"migration complete",
		"accepted=10",
		`target="articles"`,
		"rate=2.0 records/s",
		"elapsed=5s",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("CompleteLine() = %q, want containing %q", got, want)
		}
	}
}

func TestFormatPercentHandlesUnknownTotal(t *testing.T) {
	if got := formatPercent(3, 0); got != "unknown" {
		t.Fatalf("formatPercent() = %q, want unknown", got)
	}
	if got := formatPercent(0, 0); got != "100.0%" {
		t.Fatalf("formatPercent() = %q, want 100.0%%", got)
	}
}
