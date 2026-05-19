package cmd

import (
	"context"

	"github.com/lambdadb/lambdadb-migration/internal/config"
	pineconesource "github.com/lambdadb/lambdadb-migration/internal/source/pinecone"
)

type MigratePineconeCmd struct {
	Pinecone    config.PineconeConfig  `embed:"" prefix:"pinecone."`
	LambdaDB    config.LambdaDBConfig  `embed:"" prefix:"lambdadb."`
	Migration   config.MigrationConfig `embed:"" prefix:"migration."`
	MappingFile string                 `help:"Path to a JSON or YAML migration mapping file."`
}

func (c *MigratePineconeCmd) Run(globals *Globals) error {
	ctx := context.Background()
	src, err := pineconesource.New(c.Pinecone)
	if err != nil {
		return err
	}
	defer src.Close()

	return runMigration(ctx, migrationRunConfig{
		SourceKind:       "pinecone",
		SourceCollection: pineconeSourceCollection(c.Pinecone),
		Source:           src,
		LambdaDB:         c.LambdaDB,
		Migration:        c.Migration,
		MappingFile:      c.MappingFile,
	})
}

func pineconeSourceCollection(cfg config.PineconeConfig) string {
	if cfg.Namespace == "" {
		return cfg.Index
	}
	return cfg.Index + "/" + cfg.Namespace
}
