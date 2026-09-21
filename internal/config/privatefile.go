package config

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

// WritePrivateFile atomically replaces path with data using a temporary file
// in the same directory. On Unix, both the parent directory and file are
// restricted to the current operating-system user.
func WritePrivateFile(path string, data []byte) error {
	directory := filepath.Dir(path)
	if directory != "." {
		if err := ensurePrivateDir(directory); err != nil {
			return err
		}
	}

	temporary, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create private temporary file for %q: %w", path, err)
	}
	temporaryPath := temporary.Name()
	keepTemporary := true
	defer func() {
		if keepTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()

	writeErr := error(nil)
	if runtime.GOOS != "windows" {
		writeErr = temporary.Chmod(0o600)
	}
	if writeErr == nil {
		_, writeErr = temporary.Write(data)
	}
	if writeErr == nil {
		writeErr = temporary.Sync()
	}
	closeErr := temporary.Close()
	if writeErr != nil {
		return fmt.Errorf("write private temporary file for %q: %w", path, writeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close private temporary file for %q: %w", path, closeErr)
	}
	if err := replaceFile(temporaryPath, path); err != nil {
		return fmt.Errorf("replace private file %q: %w", path, err)
	}
	keepTemporary = false
	return nil
}
