package config

import "testing"

func TestMigrationConfigValidateConfig(t *testing.T) {
	tests := []struct {
		name    string
		cfg     MigrationConfig
		wantErr bool
	}{
		{
			name: "valid",
			cfg: MigrationConfig{
				BatchSize:        1,
				MaxBatchBytes:    1,
				RetryMaxAttempts: 1,
			},
		},
		{
			name: "invalid batch size",
			cfg: MigrationConfig{
				BatchSize:        0,
				MaxBatchBytes:    1,
				RetryMaxAttempts: 1,
			},
			wantErr: true,
		},
		{
			name: "invalid max batch bytes",
			cfg: MigrationConfig{
				BatchSize:        1,
				MaxBatchBytes:    0,
				RetryMaxAttempts: 1,
			},
			wantErr: true,
		},
		{
			name: "invalid retry attempts",
			cfg: MigrationConfig{
				BatchSize:        1,
				MaxBatchBytes:    1,
				RetryMaxAttempts: 0,
			},
			wantErr: true,
		},
		{
			name: "retry max delay before initial delay",
			cfg: MigrationConfig{
				BatchSize:           1,
				MaxBatchBytes:       1,
				RetryMaxAttempts:    1,
				RetryInitialDelayMS: 100,
				RetryMaxDelayMS:     99,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.ValidateConfig()
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
