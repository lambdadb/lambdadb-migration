package config

import "fmt"

type WriteMode string

const (
	WriteModeBulk   WriteMode = "bulk"
	WriteModeUpsert WriteMode = "upsert"
)

type MigrationConfig struct {
	BatchSize        int       `help:"Maximum records to read from the source per batch." default:"500"`
	MaxBatchBytes    int64     `help:"Maximum serialized LambdaDB upsert payload size per write." default:"200000000"`
	WriteMode        WriteMode `help:"LambdaDB write mode." enum:"bulk,upsert" default:"bulk"`
	Restart          bool      `help:"Ignore existing checkpoints and start from the beginning."`
	CreateCollection bool      `help:"Create the target LambdaDB collection if missing." default:"true"`
	DryRun           bool      `help:"Inspect and validate without writing documents."`
	Validate         bool      `help:"Run validation after migration."`
	CheckpointPath   string    `help:"Checkpoint directory. Defaults to .lambdadb-migration/checkpoints."`
	BatchDelayMS     int       `help:"Delay between write batches in milliseconds." default:"0"`
}

func (c MigrationConfig) ValidateConfig() error {
	if c.BatchSize < 1 {
		return fmt.Errorf("migration batch size must be greater than 0")
	}
	if c.MaxBatchBytes < 1 {
		return fmt.Errorf("migration max batch bytes must be greater than 0")
	}
	return nil
}
