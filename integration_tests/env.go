package integration_tests

import "os"

func envAny(primary string, legacy ...string) string {
	if value := os.Getenv(primary); value != "" {
		return value
	}
	for _, name := range legacy {
		if value := os.Getenv(name); value != "" {
			return value
		}
	}
	return ""
}

func envEnabled(primary string, legacy ...string) bool {
	return envAny(primary, legacy...) == "1"
}
