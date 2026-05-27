package cmd

import (
	"context"

	"github.com/lambdadb/lambdadb-migration/internal/config"
	elasticsearchsource "github.com/lambdadb/lambdadb-migration/internal/source/elasticsearch"
)

type MigrateElasticsearchCmd struct {
	Elasticsearch config.ElasticsearchConfig `embed:"" prefix:"elasticsearch."`
	LambdaDB      config.LambdaDBConfig      `embed:"" prefix:"lambdadb."`
	Migration     config.MigrationConfig     `embed:"" prefix:"migration."`
	MappingFile   string                     `help:"Path to a JSON or YAML migration mapping file."`
}

func (c *MigrateElasticsearchCmd) Run(globals *Globals) error {
	ctx := context.Background()
	src, err := elasticsearchsource.New(c.Elasticsearch)
	if err != nil {
		return err
	}
	defer src.Close()

	return runMigration(ctx, migrationRunConfig{
		SourceKind:       "elasticsearch",
		SourceCollection: c.Elasticsearch.Index,
		Source:           src,
		LambdaDB:         c.LambdaDB,
		Migration:        c.Migration,
		MappingFile:      c.MappingFile,
	})
}
