package cmd

import (
	"encoding/json"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

type outputFormat string

const (
	outputFormatJSON outputFormat = "json"
	outputFormatYAML outputFormat = "yaml"
)

func marshalOutput(path string, value any) ([]byte, error) {
	var (
		data []byte
		err  error
	)
	switch outputFormatForPath(path) {
	case outputFormatYAML:
		data, err = yaml.Marshal(value)
	default:
		data, err = json.MarshalIndent(value, "", "  ")
	}
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func outputFormatForPath(path string) outputFormat {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".yaml", ".yml":
		return outputFormatYAML
	default:
		return outputFormatJSON
	}
}

func outputFormatName(path string) string {
	return string(outputFormatForPath(path))
}
