package lambdadb

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/lambdadb/go-lambdadb/models/apierrors"
)

func TestWriteWithRetryRetriesTransientErrors(t *testing.T) {
	attempts := 0
	err := writeWithRetry(context.Background(), WriteRetryPolicy{MaxAttempts: 3}, func() error {
		attempts++
		if attempts < 3 {
			return apierrors.NewAPIError("rate limited", http.StatusTooManyRequests, "", nil)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("writeWithRetry() error = %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestWriteWithRetryStopsOnPermanentError(t *testing.T) {
	attempts := 0
	permanent := errors.New("validation failed")
	err := writeWithRetry(context.Background(), WriteRetryPolicy{MaxAttempts: 3}, func() error {
		attempts++
		return permanent
	})
	if !errors.Is(err, permanent) {
		t.Fatalf("writeWithRetry() error = %v, want permanent", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestWriteWithRetryHonorsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	err := writeWithRetry(ctx, WriteRetryPolicy{MaxAttempts: 3, InitialDelay: time.Second}, func() error {
		attempts++
		cancel()
		return apierrors.NewAPIError("temporary outage", http.StatusServiceUnavailable, "", nil)
	})
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("writeWithRetry() error = %v, want context canceled", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestIsTransientWriteError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "too many requests",
			err:  &apierrors.TooManyRequestsError{},
			want: true,
		},
		{
			name: "internal server error",
			err:  &apierrors.InternalServerError{},
			want: true,
		},
		{
			name: "api 503",
			err:  apierrors.NewAPIError("unavailable", http.StatusServiceUnavailable, "", nil),
			want: true,
		},
		{
			name: "api 400",
			err:  apierrors.NewAPIError("bad request", http.StatusBadRequest, "", nil),
			want: false,
		},
		{
			name: "bulk upload 500",
			err:  errors.New("upload failed: status 500, body: temporary outage"),
			want: true,
		},
		{
			name: "permanent",
			err:  errors.New("invalid vector dimensions"),
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isTransientWriteError(tt.err); got != tt.want {
				t.Fatalf("isTransientWriteError() = %v, want %v", got, tt.want)
			}
		})
	}
}
