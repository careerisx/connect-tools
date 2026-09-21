package config

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

const appDirectoryName = "connect-tools"

// Paths contains all files and directories managed by Connect Tools.
type Paths struct {
	ConfigDir    string
	ConfigFile   string
	ProfilesFile string
	RuntimeFile  string
	TokenFile    string
	ExportDir    string
}

// ResolvePaths resolves the platform-specific configuration paths and creates
// application-owned directories with permissions restricted to the current user.
func ResolvePaths() (Paths, error) {
	configDir, err := resolveConfigDir(runtime.GOOS, os.Getenv, os.UserHomeDir)
	if err != nil {
		return Paths{}, err
	}

	exportDir := filepath.Join(configDir, "exports")
	for _, dir := range []string{configDir, exportDir} {
		if err := ensurePrivateDir(dir); err != nil {
			return Paths{}, err
		}
	}

	return Paths{
		ConfigDir:    configDir,
		ConfigFile:   filepath.Join(configDir, "config.json"),
		ProfilesFile: filepath.Join(configDir, "profiles.json"),
		RuntimeFile:  filepath.Join(configDir, "runtime.json"),
		TokenFile:    filepath.Join(configDir, "access-token"),
		ExportDir:    exportDir,
	}, nil
}

func resolveConfigDir(goos string, getenv func(string) string, userHomeDir func() (string, error)) (string, error) {
	if goos == "windows" {
		appData := getenv("APPDATA")
		if appData == "" {
			return "", fmt.Errorf("resolve config directory: APPDATA is not set")
		}
		return filepath.Join(appData, appDirectoryName), nil
	}

	home, err := userHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve user home directory: %w", err)
	}
	if home == "" {
		return "", fmt.Errorf("resolve user home directory: empty path")
	}

	switch goos {
	case "darwin":
		return filepath.Join(home, "Library", "Application Support", appDirectoryName), nil
	case "linux":
		return filepath.Join(home, ".config", appDirectoryName), nil
	default:
		return "", fmt.Errorf("resolve config directory: unsupported operating system %q", goos)
	}
}

func ensurePrivateDir(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return fmt.Errorf("create private directory %q: %w", path, err)
	}
	if runtime.GOOS != "windows" {
		if err := os.Chmod(path, 0o700); err != nil {
			return fmt.Errorf("set private directory permissions for %q: %w", path, err)
		}
	}
	return nil
}

func ensurePrivateParent(path string) error {
	dir := filepath.Dir(path)
	if dir == "." {
		return nil
	}
	if info, err := os.Stat(dir); err == nil {
		if !info.IsDir() {
			return fmt.Errorf("create private directory %q: path exists and is not a directory", dir)
		}
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect private directory %q: %w", dir, err)
	}
	return ensurePrivateDir(dir)
}
