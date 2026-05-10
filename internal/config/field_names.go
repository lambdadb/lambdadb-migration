package config

import "strings"

func NormalizeFieldName(name string) string {
	return strings.ReplaceAll(name, ".", "_")
}

func PayloadTargetField(sourceField string, renames map[string]string) string {
	if target, ok := renames[sourceField]; ok && target != "" {
		return target
	}
	return NormalizeFieldName(sourceField)
}
