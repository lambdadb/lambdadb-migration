package lambdadb

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	sdk "github.com/lambdadb/go-lambdadb"
	"github.com/lambdadb/go-lambdadb/models/apierrors"
	"github.com/lambdadb/go-lambdadb/models/components"
	"github.com/lambdadb/lambdadb-migration/internal/config"
	"github.com/lambdadb/lambdadb-migration/internal/source"
)

const (
	collectionReadyPollInterval = 500 * time.Millisecond
	collectionReadyTimeout      = 30 * time.Second
	consistentRead              = true
	includeVectors              = true
)

var defaultWriteRetryPolicy = retryPolicy{
	maxAttempts:  5,
	initialDelay: 500 * time.Millisecond,
	maxDelay:     5 * time.Second,
}

type retryPolicy struct {
	maxAttempts  int
	initialDelay time.Duration
	maxDelay     time.Duration
}

type Target struct {
	client     *sdk.Client
	collection string
	writeMode  config.WriteMode
}

func New(cfg config.LambdaDBConfig, writeMode config.WriteMode) *Target {
	client := sdk.New(
		sdk.WithBaseURL(cfg.BaseURL),
		sdk.WithProjectName(cfg.ProjectName),
		sdk.WithAPIKey(cfg.APIKey),
	)
	return &Target{
		client:     client,
		collection: cfg.Collection,
		writeMode:  writeMode,
	}
}

func (t *Target) EnsureCollection(ctx context.Context, inv *source.Inventory, mapping config.MappingConfig) error {
	if !mapping.Target.CreateCollection {
		return nil
	}
	if collection, err := t.client.Collection(t.collection).Get(ctx); err == nil {
		return t.waitForActiveCollection(ctx, collectionReadyTimeout, collection)
	} else {
		var notFound *apierrors.ResourceNotFoundError
		if !errors.As(err, &notFound) {
			return fmt.Errorf("get LambdaDB collection: %w", err)
		}
	}

	indexConfigs, err := buildIndexConfigs(mapping)
	if err != nil {
		return err
	}
	_, err = t.client.Collections.Create(ctx, sdk.CreateCollectionOptions{
		CollectionName: t.collection,
		IndexConfigs:   indexConfigs,
	})
	if err != nil {
		return fmt.Errorf("create LambdaDB collection: %w", err)
	}
	return t.waitForActiveCollection(ctx, collectionReadyTimeout, nil)
}

func (t *Target) waitForActiveCollection(ctx context.Context, timeout time.Duration, first *components.CollectionResponse) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	collection := first
	for {
		if collection != nil {
			switch collection.CollectionStatus {
			case components.StatusActive, "":
				return nil
			case components.StatusDeleting:
				return fmt.Errorf("LambdaDB collection %q is deleting", t.collection)
			}
		}

		select {
		case <-waitCtx.Done():
			status := components.Status("")
			if collection != nil {
				status = collection.CollectionStatus
			}
			return fmt.Errorf("wait for LambdaDB collection %q to become ACTIVE: last status %q: %w", t.collection, status, waitCtx.Err())
		case <-time.After(collectionReadyPollInterval):
		}

		next, err := t.client.Collection(t.collection).Get(ctx)
		if err != nil {
			return fmt.Errorf("get LambdaDB collection while waiting for ACTIVE: %w", err)
		}
		collection = next
	}
}

func (t *Target) Write(ctx context.Context, docs []map[string]any) error {
	if len(docs) == 0 {
		return nil
	}
	return writeWithRetry(ctx, defaultWriteRetryPolicy, func() error {
		return t.writeOnce(ctx, docs)
	})
}

func (t *Target) writeOnce(ctx context.Context, docs []map[string]any) error {
	switch t.writeMode {
	case config.WriteModeBulk:
		_, err := t.client.Collection(t.collection).Docs().BulkUpsertDocuments(ctx, sdk.UpsertDocsInput{Docs: docs})
		return err
	case config.WriteModeUpsert:
		_, err := t.client.Collection(t.collection).Docs().Upsert(ctx, sdk.UpsertDocsInput{Docs: docs})
		return err
	default:
		return fmt.Errorf("unsupported LambdaDB write mode %q", t.writeMode)
	}
}

func writeWithRetry(ctx context.Context, policy retryPolicy, operation func() error) error {
	if policy.maxAttempts < 1 {
		policy.maxAttempts = 1
	}
	delay := policy.initialDelay
	if delay < 0 {
		delay = 0
	}
	maxDelay := policy.maxDelay
	if maxDelay < delay {
		maxDelay = delay
	}

	var lastErr error
	for attempt := 1; attempt <= policy.maxAttempts; attempt++ {
		if err := operation(); err != nil {
			lastErr = err
			if attempt == policy.maxAttempts || !isTransientWriteError(err) {
				return err
			}
		} else {
			return nil
		}

		if delay == 0 {
			continue
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("retry LambdaDB write after transient error: %w: %w", lastErr, ctx.Err())
		case <-timer.C:
		}
		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}
	return lastErr
}

func isTransientWriteError(err error) bool {
	if err == nil {
		return false
	}
	var tooManyRequests *apierrors.TooManyRequestsError
	if errors.As(err, &tooManyRequests) {
		return true
	}
	var internalServer *apierrors.InternalServerError
	if errors.As(err, &internalServer) {
		return true
	}
	var apiErr *apierrors.APIError
	if errors.As(err, &apiErr) {
		return apiErr.StatusCode == 429 || apiErr.StatusCode >= 500
	}
	var netErr net.Error
	if errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary()) {
		return true
	}

	message := strings.ToLower(err.Error())
	for _, needle := range []string{
		"connection reset",
		"connection refused",
		"temporary",
		"timeout",
		"upload failed: status 429",
		"upload failed: status 5",
	} {
		if strings.Contains(message, needle) {
			return true
		}
	}
	return false
}

func (t *Target) Count(ctx context.Context) (uint64, error) {
	meta, err := t.client.Collection(t.collection).Get(ctx)
	if err != nil {
		return 0, err
	}
	if meta == nil {
		return 0, nil
	}
	return uint64(meta.NumDocs), nil
}

func (t *Target) Fetch(ctx context.Context, ids []string) ([]map[string]any, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	result, err := t.client.Collection(t.collection).Docs().Fetch(ctx, sdk.FetchDocsInput{
		Ids:            ids,
		ConsistentRead: sdk.Bool(consistentRead),
		IncludeVectors: sdk.Bool(includeVectors),
	})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil
	}
	docs := make([]map[string]any, 0, len(result.Docs))
	for _, doc := range result.Docs {
		docs = append(docs, doc.Doc)
	}
	return docs, nil
}
