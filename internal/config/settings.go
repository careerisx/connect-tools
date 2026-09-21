package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

const settingsSchemaVersion = 1

// Settings contains user-controlled local service settings.
type Settings struct {
	SchemaVersion int  `json:"schema_version"`
	MCPEnabled    bool `json:"mcp_enabled"`
}

// LoadSettings reads strict settings or returns secure defaults when absent.
func LoadSettings(path string) (Settings, error) {
	defaults := Settings{SchemaVersion: settingsSchemaVersion}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return defaults, nil
	}
	if err != nil {
		return Settings{}, fmt.Errorf("read settings: %w", err)
	}
	var value Settings
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return Settings{}, fmt.Errorf("decode settings: %w", err)
	}
	if value.SchemaVersion != settingsSchemaVersion {
		return Settings{}, fmt.Errorf("unsupported settings schema version %d", value.SchemaVersion)
	}
	return value, nil
}

// SaveSettings atomically persists settings with private permissions.
func SaveSettings(path string, value Settings) error {
	value.SchemaVersion = settingsSchemaVersion
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode settings: %w", err)
	}
	data = append(data, '\n')
	return WritePrivateFile(path, data)
}
