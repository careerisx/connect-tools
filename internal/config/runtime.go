package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"
)

// RuntimeInfo describes the currently running Connect Tools server.
type RuntimeInfo struct {
	PID        int       `json:"pid"`
	Address    string    `json:"address"`
	InstanceID string    `json:"instance_id"`
	Version    string    `json:"version"`
	StartedAt  time.Time `json:"started_at"`
}

// WriteRuntime atomically replaces the runtime metadata file.
func WriteRuntime(path string, info RuntimeInfo) error {
	data, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal runtime metadata: %w", err)
	}
	data = append(data, '\n')

	if err := WritePrivateFile(path, data); err != nil {
		return fmt.Errorf("write runtime metadata: %w", err)
	}
	return nil
}

// ReadRuntime reads runtime metadata from disk.
func ReadRuntime(path string) (RuntimeInfo, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return RuntimeInfo{}, fmt.Errorf("read runtime metadata file %q: %w", path, err)
	}

	var info RuntimeInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return RuntimeInfo{}, fmt.Errorf("decode runtime metadata file %q: %w", path, err)
	}
	return info, nil
}

// RemoveRuntime removes stale runtime metadata during server shutdown. A
// missing file is treated as already removed.
func RemoveRuntime(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove runtime metadata file %q: %w", path, err)
	}
	return nil
}
