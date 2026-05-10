package lambdadb

import (
	"context"
	"errors"
	"fmt"

	sdk "github.com/lambdadb/go-lambdadb"
	"github.com/lambdadb/go-lambdadb/models/apierrors"
	"github.com/lambdadb/lambdadb-migration/internal/config"
	"github.com/lambdadb/lambdadb-migration/internal/source"
)

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
	if _, err := t.client.Collection(t.collection).Get(ctx); err == nil {
		return nil
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
	return nil
}

func (t *Target) Write(ctx context.Context, docs []map[string]any) error {
	if len(docs) == 0 {
		return nil
	}
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
