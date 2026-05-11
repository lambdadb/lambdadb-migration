package config

import "fmt"

type WriteMode string

const (
	WriteModeBulk   WriteMode = "bulk"
	WriteModeUpsert WriteMode = "upsert"
)

type MigrationConfig struct {
	BatchSize            int       `help:"Maximum records to read from the source per batch." default:"500"`
	MaxBatchBytes        int64     `help:"Maximum serialized LambdaDB upsert payload size per write." default:"200000000"`
	WriteMode            WriteMode `help:"LambdaDB write mode." enum:"bulk,upsert" default:"bulk"`
	Restart              bool      `help:"Ignore existing checkpoints and start from the beginning."`
	CreateCollection     *bool     `help:"Create the target LambdaDB collection if missing. Set to false to require an existing collection. Overrides target.createCollection in the mapping file."`
	DryRun               bool      `help:"Inspect and validate without writing documents."`
	Validate             bool      `help:"Run validation after migration."`
	ValidationSampleSize int       `help:"Number of migrated sample documents to fetch during validation. Set to 0 for count-only validation." default:"10"`
	ValidationReport     string    `help:"Write post-migration validation results to a JSON report file. Implies validation."`
	QueryOverlap         bool      `help:"Compare Qdrant and LambdaDB dense-vector query results for validation samples."`
	QueryOverlapLimit    int       `help:"Nearest-neighbor result size for query overlap validation." default:"5"`
	QueryOverlapMinRatio float64   `help:"Minimum average query overlap ratio required. Set to 0 to report without failing." default:"0"`
	CheckpointPath       string    `help:"Checkpoint directory. Defaults to .lambdadb-migration/checkpoints."`
	CleanupCheckpoint    bool      `help:"Delete the migration checkpoint after a successful migration."`
	BatchDelayMS         int       `help:"Delay between write batches in milliseconds." default:"0"`
	RetryMaxAttempts     int       `help:"Maximum LambdaDB write attempts for transient failures." default:"5"`
	RetryInitialDelayMS  int       `help:"Initial LambdaDB write retry delay in milliseconds." default:"500"`
	RetryMaxDelayMS      int       `help:"Maximum LambdaDB write retry delay in milliseconds." default:"5000"`
}

func (c MigrationConfig) ApplyToMapping(mapping MappingConfig) MappingConfig {
	if c.CreateCollection != nil {
		mapping.Target.CreateCollection = *c.CreateCollection
	}
	return mapping
}

func (c MigrationConfig) ValidateConfig() error {
	if c.BatchSize < 1 {
		return fmt.Errorf("migration batch size must be greater than 0")
	}
	if c.MaxBatchBytes < 1 {
		return fmt.Errorf("migration max batch bytes must be greater than 0")
	}
	if c.ValidationSampleSize < 0 {
		return fmt.Errorf("migration validation sample size must be greater than or equal to 0")
	}
	if c.QueryOverlap && c.QueryOverlapLimit < 1 {
		return fmt.Errorf("migration query overlap limit must be greater than 0")
	}
	if c.QueryOverlapMinRatio < 0 || c.QueryOverlapMinRatio > 1 {
		return fmt.Errorf("migration query overlap minimum ratio must be between 0 and 1")
	}
	if c.BatchDelayMS < 0 {
		return fmt.Errorf("migration batch delay must be greater than or equal to 0")
	}
	if c.RetryMaxAttempts < 1 {
		return fmt.Errorf("migration retry max attempts must be greater than 0")
	}
	if c.RetryInitialDelayMS < 0 {
		return fmt.Errorf("migration retry initial delay must be greater than or equal to 0")
	}
	if c.RetryMaxDelayMS < 0 {
		return fmt.Errorf("migration retry max delay must be greater than or equal to 0")
	}
	if c.RetryMaxDelayMS < c.RetryInitialDelayMS {
		return fmt.Errorf("migration retry max delay must be greater than or equal to initial delay")
	}
	return nil
}
