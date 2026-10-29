package config

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

const (
	tokenByteLength = 32
	tokenWaitLimit  = time.Second
	tokenWaitDelay  = 5 * time.Millisecond
)

// LoadOrCreateToken returns the persisted access token, creating a private
// cryptographically random token when the file does not exist.
func LoadOrCreateToken(path string) (string, error) {
	token, err := readToken(path)
	if err == nil {
		return token, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}

	if err := ensurePrivateParent(path); err != nil {
		return "", err
	}

	raw := make([]byte, tokenByteLength)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate access token: %w", err)
	}
	token = hex.EncodeToString(raw)

	file, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return "", fmt.Errorf("create access token temporary file for %q: %w", path, err)
	}
	temporaryPath := file.Name()
	defer func() { _ = os.Remove(temporaryPath) }()

	writeErr := error(nil)
	if runtime.GOOS != "windows" {
		writeErr = file.Chmod(0o600)
	}
	if writeErr == nil {
		_, writeErr = file.WriteString(token + "\n")
	}
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil {
		return "", fmt.Errorf("write access token temporary file for %q: %w", path, writeErr)
	}
	if closeErr != nil {
		return "", fmt.Errorf("close access token temporary file for %q: %w", path, closeErr)
	}

	if err := os.Link(temporaryPath, path); errors.Is(err, os.ErrExist) {
		return waitForToken(path)
	} else if err != nil {
		return "", fmt.Errorf("publish access token file %q: %w", path, err)
	}

	return token, nil
}

func waitForToken(path string) (string, error) {
	deadline := time.Now().Add(tokenWaitLimit)
	var lastErr error
	for {
		token, err := readToken(path)
		if err == nil {
			return token, nil
		}
		lastErr = err
		if !time.Now().Before(deadline) {
			return "", fmt.Errorf("wait for concurrently created access token %q: %w", path, lastErr)
		}
		time.Sleep(tokenWaitDelay)
	}
}

// ValidateToken 校验服务令牌，允许至少 32 个不含空格的可打印 ASCII 字符。
// 校验错误不包含令牌内容，避免凭据通过日志泄露。
func ValidateToken(token string) error {
	if len(token) < tokenByteLength {
		return fmt.Errorf("access token must contain at least %d printable ASCII characters", tokenByteLength)
	}
	for i := 0; i < len(token); i++ {
		if token[i] < '!' || token[i] > '~' {
			return errors.New("access token must contain only printable ASCII characters without spaces")
		}
	}
	return nil
}

// readToken 读取已保存的令牌，兼容自动生成和自定义的令牌格式。
func readToken(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read access token file %q: %w", path, err)
	}

	contents := string(data)
	token := contents
	if len(contents) > 0 && contents[len(contents)-1] == '\n' {
		token = contents[:len(contents)-1]
	}
	if err := ValidateToken(token); err != nil {
		return "", fmt.Errorf("invalid access token file %q: %w", path, err)
	}

	if runtime.GOOS != "windows" {
		if err := os.Chmod(path, 0o600); err != nil {
			return "", fmt.Errorf("set private access token permissions for %q: %w", path, err)
		}
	}
	return token, nil
}
