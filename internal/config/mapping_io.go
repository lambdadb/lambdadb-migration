package config

import (
	"encoding/json"
	"fmt"

	"gopkg.in/yaml.v3"
)

func DecodeMapping(data []byte) (MappingConfig, error) {
	if mapping, err := decodeMappingJSON(data); err == nil {
		return mapping, nil
	}
	if mapping, err := decodeMappingYAML(data); err == nil {
		return mapping, nil
	}
	return MappingConfig{}, fmt.Errorf("mapping file must contain either a direct mapping or an inventory output with a mapping field")
}

func decodeMappingJSON(data []byte) (MappingConfig, error) {
	var direct MappingConfig
	if err := json.Unmarshal(data, &direct); err == nil && direct.Target.Collection != "" {
		return direct, nil
	}
	var wrapped struct {
		Mapping MappingConfig `json:"mapping"`
	}
	if err := json.Unmarshal(data, &wrapped); err != nil {
		return MappingConfig{}, err
	}
	if wrapped.Mapping.Target.Collection == "" {
		return MappingConfig{}, fmt.Errorf("missing target.collection")
	}
	return wrapped.Mapping, nil
}

func decodeMappingYAML(data []byte) (MappingConfig, error) {
	var direct MappingConfig
	if err := yaml.Unmarshal(data, &direct); err == nil && direct.Target.Collection != "" {
		return direct, nil
	}
	var wrapped struct {
		Mapping MappingConfig `yaml:"mapping"`
	}
	if err := yaml.Unmarshal(data, &wrapped); err != nil {
		return MappingConfig{}, err
	}
	if wrapped.Mapping.Target.Collection == "" {
		return MappingConfig{}, fmt.Errorf("missing target.collection")
	}
	return wrapped.Mapping, nil
}
