package cmd

import (
	"fmt"
	"time"
)

type progressTracker struct {
	total         uint64
	startAccepted uint64
	startedAt     time.Time
}

func newProgressTracker(total, startAccepted uint64, startedAt time.Time) progressTracker {
	return progressTracker{
		total:         total,
		startAccepted: startAccepted,
		startedAt:     startedAt,
	}
}

func (p progressTracker) BatchLine(accepted uint64, batchSize int, now time.Time) string {
	elapsed := now.Sub(p.startedAt)
	return fmt.Sprintf(
		"progress accepted=%d/%d (%s) batch=%d rate=%s elapsed=%s",
		accepted,
		p.total,
		formatPercent(accepted, p.total),
		batchSize,
		formatRate(accepted-p.startAccepted, elapsed),
		formatDuration(elapsed),
	)
}

func (p progressTracker) CompleteLine(accepted uint64, collection string, now time.Time) string {
	elapsed := now.Sub(p.startedAt)
	return fmt.Sprintf(
		"migration complete accepted=%d target=%q rate=%s elapsed=%s",
		accepted,
		collection,
		formatRate(accepted-p.startAccepted, elapsed),
		formatDuration(elapsed),
	)
}

func formatPercent(done, total uint64) string {
	if total == 0 {
		if done == 0 {
			return "100.0%"
		}
		return "unknown"
	}
	percent := float64(done) * 100 / float64(total)
	if percent > 100 {
		percent = 100
	}
	return fmt.Sprintf("%.1f%%", percent)
}

func formatRate(done uint64, elapsed time.Duration) string {
	if done == 0 || elapsed <= 0 {
		return "0.0 records/s"
	}
	rate := float64(done) / elapsed.Seconds()
	return fmt.Sprintf("%.1f records/s", rate)
}

func formatDuration(value time.Duration) string {
	if value < 0 {
		value = 0
	}
	if value < time.Second {
		return value.Truncate(time.Millisecond).String()
	}
	if value < time.Minute {
		return value.Truncate(100 * time.Millisecond).String()
	}
	return value.Truncate(time.Second).String()
}
