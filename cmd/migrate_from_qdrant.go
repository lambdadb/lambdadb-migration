package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
	MappingFile string                 `help:"Path to a migration mapping file."`
}

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
	if c.Migration.DryRun {
		return printDryRun(inv.RecordCount, mapping)
	}

	target := targetlambdadb.New(c.LambdaDB, c.Migration.WriteMode)
	if err := target.EnsureCollection(ctx, inv, mapping); err != nil {
		return err
	}
	maxBatchBytes := targetlambdadb.EffectiveMaxBatchBytes(c.Migration.MaxBatchBytes, c.Migration.WriteMode)

	store := checkpoint.NewFileStore(checkpointRoot(c.Migration.CheckpointPath))
	key := checkpointKey(c.Qdrant.Collection, c.LambdaDB.ProjectName, c.LambdaDB.Collection)
	var cursorValue any
	var accepted uint64
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

		fmt.Fprintf(os.Stderr, "accepted %d/%d records\n", accepted, inv.RecordCount)
		if batch.Done {
			break
		}
		if c.Migration.BatchDelayMS > 0 {
			time.Sleep(time.Duration(c.Migration.BatchDelayMS) * time.Millisecond)
		}
	}

	fmt.Fprintf(os.Stderr, "migration accepted %d records into LambdaDB collection %q\n", accepted, c.LambdaDB.Collection)
	return nil
}

func (c *MigrateQdrantCmd) loadMapping(inv *source.Inventory) (config.MappingConfig, error) {
	if c.MappingFile == "" {
		return config.MappingFromInventory(inv, c.LambdaDB.Collection), nil
	}
	data, err := os.ReadFile(c.MappingFile)
	if err != nil {
		return config.MappingConfig{}, fmt.Errorf("read mapping file: %w", err)
	}
	var direct config.MappingConfig
	if err := json.Unmarshal(data, &direct); err == nil && direct.Target.Collection != "" {
		return direct, nil
	}
	var wrapped struct {
		Mapping config.MappingConfig `json:"mapping"`
	}
	if err := json.Unmarshal(data, &wrapped); err != nil {
		return config.MappingConfig{}, fmt.Errorf("decode mapping file: %w", err)
	}
	if wrapped.Mapping.Target.Collection == "" {
		return config.MappingConfig{}, fmt.Errorf("mapping file does not contain target.collection")
	}
	return wrapped.Mapping, nil
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
