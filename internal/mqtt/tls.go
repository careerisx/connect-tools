package mqtt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"runtime"
	"strings"

	"connect-tools/internal/secrets"

	pkcs8 "github.com/youmark/pkcs8"
)

const (
	// MaxTLSFileBytes limits every CA, certificate, and private-key file read.
	MaxTLSFileBytes int64 = 8 << 20
)

var (
	// ErrTLSFileTooLarge indicates that a certificate-related file exceeds MaxTLSFileBytes.
	ErrTLSFileTooLarge = errors.New("TLS file exceeds size limit")
	// ErrInvalidTLSConfiguration indicates inconsistent or unusable TLS input.
	ErrInvalidTLSConfiguration = errors.New("invalid TLS configuration")
	// ErrInvalidTLSMaterial indicates malformed CA, certificate, or private-key data.
	ErrInvalidTLSMaterial = errors.New("invalid TLS certificate material")
	// ErrPrivateKeyPassphraseRequired indicates that an encrypted key has no credential reference.
	ErrPrivateKeyPassphraseRequired = errors.New("private-key passphrase is required")
	// ErrPrivateKeyDecryption indicates that an encrypted private key could not be decrypted.
	ErrPrivateKeyDecryption = errors.New("private-key decryption failed")
)

// TLSProfile contains the MQTT endpoint and certificate inputs needed to build
// a TLS configuration without depending on persistent profile or Paho types.
type TLSProfile struct {
	// Enabled controls whether a TLS configuration is required.
	Enabled bool
	// BrokerHost supplies the verification name when ServerName is empty.
	BrokerHost string
	// CAFile names an optional PEM bundle appended to the system roots.
	CAFile string
	// ClientCertFile names an optional PEM client certificate chain.
	ClientCertFile string
	// ClientKeyFile names the private key paired with ClientCertFile.
	ClientKeyFile string
	// ServerName overrides BrokerHost for certificate verification.
	ServerName string
	// PrivateKeyPassphraseRef identifies a managed passphrase for an encrypted key.
	PrivateKeyPassphraseRef string
}

// BuildTLSConfig constructs a TLS 1.2-or-newer configuration. It never enables
// insecure verification and never falls back to a plaintext transport.
func BuildTLSConfig(ctx context.Context, profile TLSProfile, secretStore secrets.Store) (*tls.Config, error) {
	if !profile.Enabled {
		return nil, nil
	}
	if err := checkContext(ctx); err != nil {
		return nil, DiagnoseTLSError(StageValidate, err)
	}
	if (profile.ClientCertFile == "") != (profile.ClientKeyFile == "") {
		return nil, newDiagnostic(
			StageValidate,
			CodeInvalidConfiguration,
			"client certificate and private-key files must be configured together",
			false,
			ErrInvalidTLSConfiguration,
		)
	}
	if profile.PrivateKeyPassphraseRef != "" && profile.ClientKeyFile == "" {
		return nil, newDiagnostic(
			StageValidate,
			CodeInvalidConfiguration,
			"a private-key passphrase requires a client private-key file",
			false,
			ErrInvalidTLSConfiguration,
		)
	}

	serverName, err := resolveServerName(profile.ServerName, profile.BrokerHost)
	if err != nil {
		return nil, err
	}
	config := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: serverName,
	}

	if profile.CAFile != "" {
		roots, rootErr := x509.SystemCertPool()
		if rootErr != nil || roots == nil {
			roots = x509.NewCertPool()
		}
		caPEM, readErr := readLimitedTLSFile(ctx, profile.CAFile, StageLoadCA, "CA certificate")
		if readErr != nil {
			return nil, readErr
		}
		defer zeroBytes(caPEM)
		appended := roots.AppendCertsFromPEM(caPEM)
		if err := checkContext(ctx); err != nil {
			return nil, DiagnoseTLSError(StageLoadCA, err)
		}
		if !appended {
			return nil, newDiagnostic(
				StageLoadCA,
				CodeInvalidCA,
				"CA file "+safeBase(profile.CAFile)+" contains no valid certificates",
				false,
				ErrInvalidTLSMaterial,
			)
		}
		config.RootCAs = roots
	}

	if profile.ClientCertFile != "" {
		certificatePEM, readErr := readLimitedTLSFile(ctx, profile.ClientCertFile, StageLoadClientCertificate, "client certificate")
		if readErr != nil {
			return nil, readErr
		}
		defer zeroBytes(certificatePEM)
		privateKeyPEM, readErr := readLimitedTLSFile(ctx, profile.ClientKeyFile, StageLoadClientCertificate, "client private key")
		if readErr != nil {
			return nil, readErr
		}
		defer zeroBytes(privateKeyPEM)

		usableKeyPEM, prepared, prepareErr := preparePrivateKey(
			ctx,
			privateKeyPEM,
			profile.PrivateKeyPassphraseRef,
			secretStore,
		)
		if prepareErr != nil {
			return nil, prepareErr
		}
		if prepared {
			defer zeroBytes(usableKeyPEM)
		}
		certificate, pairErr := tls.X509KeyPair(certificatePEM, usableKeyPEM)
		if pairErr != nil {
			return nil, newDiagnostic(
				StageLoadClientCertificate,
				CodeInvalidClientCertificate,
				"client certificate or private key is invalid",
				false,
				fmt.Errorf("%w: %v", ErrInvalidTLSMaterial, pairErr),
			)
		}
		config.Certificates = []tls.Certificate{certificate}
	}

	if err := checkContext(ctx); err != nil {
		return nil, DiagnoseTLSError(StageValidate, err)
	}
	return config, nil
}

func resolveServerName(explicit, brokerHost string) (string, error) {
	serverName := explicit
	if serverName == "" {
		serverName = brokerHost
	}
	if !validTLSServerName(serverName) {
		return "", newDiagnostic(
			StageResolveServerName,
			CodeInvalidServerName,
			"TLS server name must be a valid hostname or IP address without a port",
			false,
			ErrInvalidTLSConfiguration,
		)
	}
	return serverName, nil
}

func validTLSServerName(value string) bool {
	if value == "" || strings.TrimSpace(value) != value || strings.ContainsRune(value, 0) || strings.Contains(value, "://") {
		return false
	}
	if net.ParseIP(value) != nil {
		return true
	}
	if strings.ContainsAny(value, ":/\\[]%") {
		return false
	}
	name := strings.TrimSuffix(value, ".")
	if name == "" || len(name) > 253 {
		return false
	}
	for _, label := range strings.Split(name, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if (character < 'a' || character > 'z') &&
				(character < 'A' || character > 'Z') &&
				(character < '0' || character > '9') && character != '-' {
				return false
			}
		}
	}
	return true
}

func readLimitedTLSFile(ctx context.Context, path string, stage DiagnosticStage, kind string) ([]byte, error) {
	if err := checkContext(ctx); err != nil {
		return nil, DiagnoseTLSError(stage, err)
	}
	file, openErr := os.Open(path)
	contextAfterOpen := checkContext(ctx)
	if openErr != nil || contextAfterOpen != nil {
		var closeErr error
		if file != nil {
			closeErr = file.Close()
		}
		if contextAfterClose := checkContext(ctx); contextAfterClose != nil {
			return nil, DiagnoseTLSError(stage, contextAfterClose)
		}
		if contextAfterOpen != nil {
			return nil, DiagnoseTLSError(stage, contextAfterOpen)
		}
		if openErr != nil {
			return nil, fileDiagnostic(stage, kind, path, openErr)
		}
		return nil, fileDiagnostic(stage, kind, path, closeErr)
	}

	data, readErr := io.ReadAll(io.LimitReader(file, MaxTLSFileBytes+1))
	contextAfterRead := checkContext(ctx)
	if readErr != nil || contextAfterRead != nil {
		closeErr := file.Close()
		contextAfterClose := checkContext(ctx)
		zeroBytes(data)
		if contextAfterClose != nil {
			return nil, DiagnoseTLSError(stage, contextAfterClose)
		}
		if contextAfterRead != nil {
			return nil, DiagnoseTLSError(stage, contextAfterRead)
		}
		if readErr != nil {
			return nil, fileDiagnostic(stage, kind, path, readErr)
		}
		return nil, fileDiagnostic(stage, kind, path, closeErr)
	}
	closeErr := file.Close()
	contextAfterClose := checkContext(ctx)
	if contextAfterClose != nil {
		zeroBytes(data)
		return nil, DiagnoseTLSError(stage, contextAfterClose)
	}
	if closeErr != nil {
		zeroBytes(data)
		return nil, fileDiagnostic(stage, kind, path, closeErr)
	}
	if int64(len(data)) > MaxTLSFileBytes {
		zeroBytes(data)
		return nil, newDiagnostic(
			stage,
			CodeFileTooLarge,
			kind+" file "+safeBase(path)+" exceeds the size limit",
			false,
			ErrTLSFileTooLarge,
		)
	}
	return data, nil
}

func fileDiagnostic(stage DiagnosticStage, kind, path string, err error) *DiagnosticError {
	return newDiagnostic(
		stage,
		CodeFileUnavailable,
		kind+" file "+safeBase(path)+" is unavailable",
		false,
		safeFileCause(err),
	)
}

func safeFileCause(err error) error {
	var pathError *os.PathError
	if errors.As(err, &pathError) {
		return fmt.Errorf("%s %s: %w", pathError.Op, safeBase(pathError.Path), pathError.Err)
	}
	if errors.Is(err, os.ErrNotExist) {
		return os.ErrNotExist
	}
	if errors.Is(err, os.ErrPermission) {
		return os.ErrPermission
	}
	return errors.New("certificate file operation failed")
}

func preparePrivateKey(ctx context.Context, keyPEM []byte, passphraseRef string, secretStore secrets.Store) ([]byte, bool, error) {
	rest := keyPEM
	for len(rest) > 0 {
		block, remaining := pem.Decode(rest)
		if block == nil {
			break
		}
		rest = remaining
		if !isPrivateKeyBlock(block.Type) {
			zeroBytes(block.Bytes)
			continue
		}
		if block.Type != "ENCRYPTED PRIVATE KEY" && !x509.IsEncryptedPEMBlock(block) {
			zeroBytes(block.Bytes)
			return keyPEM, false, nil
		}

		passphrase, err := resolvePrivateKeyPassphrase(ctx, passphraseRef, secretStore)
		if err != nil {
			zeroBytes(block.Bytes)
			return nil, false, err
		}

		if block.Type == "ENCRYPTED PRIVATE KEY" {
			parsedKey, parseErr := pkcs8.ParsePKCS8PrivateKey(block.Bytes, passphrase)
			zeroBytes(block.Bytes)
			zeroBytes(passphrase)
			if parseErr != nil {
				return nil, false, privateKeyDecryptionDiagnostic()
			}
			decryptedDER, marshalErr := x509.MarshalPKCS8PrivateKey(parsedKey)
			if marshalErr != nil {
				return nil, false, privateKeyDecryptionDiagnostic()
			}
			decryptedPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: decryptedDER})
			zeroBytes(decryptedDER)
			if len(decryptedPEM) == 0 {
				return nil, false, privateKeyDecryptionDiagnostic()
			}
			return decryptedPEM, true, nil
		}

		decryptedDER, decryptErr := x509.DecryptPEMBlock(block, passphrase)
		zeroBytes(block.Bytes)
		zeroBytes(passphrase)
		if decryptErr != nil {
			return nil, false, privateKeyDecryptionDiagnostic()
		}
		decryptedPEM := pem.EncodeToMemory(&pem.Block{Type: block.Type, Bytes: decryptedDER})
		zeroBytes(decryptedDER)
		if len(decryptedPEM) == 0 {
			return nil, false, privateKeyDecryptionDiagnostic()
		}
		return decryptedPEM, true, nil
	}
	return keyPEM, false, nil
}

func resolvePrivateKeyPassphrase(ctx context.Context, ref string, secretStore secrets.Store) ([]byte, error) {
	if ref == "" {
		return nil, newDiagnostic(
			StageResolvePrivateKeyPassphrase,
			CodePassphraseRequired,
			"encrypted client private key requires a passphrase",
			false,
			ErrPrivateKeyPassphraseRequired,
		)
	}
	if secretStore == nil {
		return nil, newDiagnostic(
			StageResolvePrivateKeyPassphrase,
			CodeCredentialUnavailable,
			"private-key passphrase is unavailable",
			false,
			secrets.ErrUnavailable,
		)
	}
	if err := checkContext(ctx); err != nil {
		return nil, DiagnoseTLSError(StageResolvePrivateKeyPassphrase, err)
	}
	passphrase, err := secretStore.Get(ctx, ref)
	contextAfterGet := checkContext(ctx)
	if contextAfterGet != nil {
		zeroBytes(passphrase)
		return nil, DiagnoseTLSError(StageResolvePrivateKeyPassphrase, contextAfterGet)
	}
	if err != nil {
		zeroBytes(passphrase)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, DiagnoseTLSError(StageResolvePrivateKeyPassphrase, err)
		}
		return nil, newDiagnostic(
			StageResolvePrivateKeyPassphrase,
			CodeCredentialUnavailable,
			"private-key passphrase is unavailable",
			false,
			safeCredentialCause(err),
		)
	}
	return passphrase, nil
}

func privateKeyDecryptionDiagnostic() *DiagnosticError {
	return newDiagnostic(
		StageLoadClientCertificate,
		CodePrivateKeyDecryptionFailed,
		"client private key could not be decrypted",
		false,
		ErrPrivateKeyDecryption,
	)
}

func safeCredentialCause(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return context.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded
	case errors.Is(err, secrets.ErrNotFound):
		return secrets.ErrNotFound
	case errors.Is(err, secrets.ErrInvalidReference):
		return secrets.ErrInvalidReference
	case errors.Is(err, secrets.ErrUnavailable):
		return secrets.ErrUnavailable
	default:
		return errors.New("credential store operation failed")
	}
}

func isPrivateKeyBlock(blockType string) bool {
	switch blockType {
	case "RSA PRIVATE KEY", "EC PRIVATE KEY", "PRIVATE KEY", "ENCRYPTED PRIVATE KEY":
		return true
	default:
		return false
	}
}

func safeBase(path string) string {
	base := pathpkgBase(path)
	if base == "." || base == "/" || base == "" {
		return "certificate"
	}
	return base
}

func pathpkgBase(value string) string {
	return path.Base(strings.ReplaceAll(value, "\\", "/"))
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

func zeroBytes(value []byte) {
	clear(value)
	runtime.KeepAlive(value)
}
