// Package mqtt implements protocol-neutral MQTT connection primitives.
package mqtt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"os"
	"strings"
	"time"
)

// DiagnosticStage identifies the operation that failed while preparing or
// establishing an MQTT TLS connection.
type DiagnosticStage string

const (
	// StageValidate covers inconsistent TLS configuration.
	StageValidate DiagnosticStage = "validate"
	// StageLoadCA covers reading and parsing an additional CA bundle.
	StageLoadCA DiagnosticStage = "load_ca"
	// StageLoadClientCertificate covers reading or parsing a client identity.
	StageLoadClientCertificate DiagnosticStage = "load_client_certificate"
	// StageResolvePrivateKeyPassphrase covers credential-store access for an encrypted key.
	StageResolvePrivateKeyPassphrase DiagnosticStage = "resolve_private_key_passphrase"
	// StageResolveServerName covers selection and validation of the TLS verification name.
	StageResolveServerName DiagnosticStage = "resolve_server_name"
	// StageHandshake covers TLS negotiation and peer certificate verification.
	StageHandshake DiagnosticStage = "handshake"
)

// DiagnosticCode is a stable, machine-readable TLS failure category.
type DiagnosticCode string

const (
	// CodeInvalidConfiguration indicates inconsistent TLS settings.
	CodeInvalidConfiguration DiagnosticCode = "invalid_configuration"
	// CodeInvalidServerName indicates a name that cannot be verified by TLS.
	CodeInvalidServerName DiagnosticCode = "invalid_server_name"
	// CodeFileUnavailable indicates an unreadable certificate-related file.
	CodeFileUnavailable DiagnosticCode = "file_unavailable"
	// CodeFileTooLarge indicates a certificate-related file above the safety limit.
	CodeFileTooLarge DiagnosticCode = "file_too_large"
	// CodeInvalidCA indicates a CA file without a usable certificate.
	CodeInvalidCA DiagnosticCode = "invalid_ca"
	// CodeInvalidClientCertificate indicates an unusable client identity.
	CodeInvalidClientCertificate DiagnosticCode = "invalid_client_certificate"
	// CodePassphraseRequired indicates an encrypted key without a passphrase reference.
	CodePassphraseRequired DiagnosticCode = "private_key_passphrase_required"
	// CodeCredentialUnavailable indicates that a referenced passphrase could not be read.
	CodeCredentialUnavailable DiagnosticCode = "credential_unavailable"
	// CodePrivateKeyDecryptionFailed indicates a wrong or unsupported key passphrase.
	CodePrivateKeyDecryptionFailed DiagnosticCode = "private_key_decryption_failed"
	// CodeUnknownAuthority indicates that the broker chain is not trusted.
	CodeUnknownAuthority DiagnosticCode = "unknown_authority"
	// CodeHostnameMismatch indicates that the broker identity does not match ServerName.
	CodeHostnameMismatch DiagnosticCode = "hostname_mismatch"
	// CodeCertificateExpired indicates an expired broker certificate.
	CodeCertificateExpired DiagnosticCode = "certificate_expired"
	// CodeCertificateNotYetValid indicates a broker certificate whose validity has not begun.
	CodeCertificateNotYetValid DiagnosticCode = "certificate_not_yet_valid"
	// CodeBadCertificate indicates another certificate validation or rejection failure.
	CodeBadCertificate DiagnosticCode = "bad_certificate"
	// CodeTimeout indicates a deadline or network timeout.
	CodeTimeout DiagnosticCode = "timeout"
	// CodeTemporary indicates a network error explicitly reported as temporary.
	CodeTemporary DiagnosticCode = "temporary"
	// CodeCanceled indicates caller cancellation.
	CodeCanceled DiagnosticCode = "canceled"
	// CodeHandshakeFailed indicates a non-retryable, otherwise unclassified TLS handshake failure.
	CodeHandshakeFailed DiagnosticCode = "handshake_failed"
)

// DiagnosticError exposes a safe summary while retaining an internal cause
// for errors.Is and errors.As. Message must never contain credentials,
// credential references, or absolute certificate paths.
type DiagnosticError struct {
	Stage     DiagnosticStage `json:"stage"`
	Code      DiagnosticCode  `json:"code"`
	Message   string          `json:"message"`
	Retryable bool            `json:"retryable"`
	cause     error
}

// Error returns the safe, user-facing diagnostic summary.
func (e *DiagnosticError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message == "" {
		return string(e.Code)
	}
	return e.Message
}

// Unwrap returns the internal cause for errors.Is and errors.As.
func (e *DiagnosticError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// DiagnoseTLSError maps a TLS, x509, context, or network failure into a safe
// structured diagnostic. Existing DiagnosticError values are preserved.
func DiagnoseTLSError(stage DiagnosticStage, err error) *DiagnosticError {
	if err == nil {
		return nil
	}
	var existing *DiagnosticError
	if errors.As(err, &existing) {
		return existing
	}
	if errors.Is(err, context.Canceled) {
		return newDiagnostic(stage, CodeCanceled, "TLS operation was canceled", false, context.Canceled)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return newDiagnostic(stage, CodeTimeout, "TLS operation timed out", true, context.DeadlineExceeded)
	}
	var pathError *os.PathError
	if errors.As(err, &pathError) {
		return newDiagnostic(stage, CodeFileUnavailable, "TLS certificate file is unavailable", false, safeFileCause(pathError))
	}

	var unknownAuthority x509.UnknownAuthorityError
	if errors.As(err, &unknownAuthority) {
		return newDiagnostic(stage, CodeUnknownAuthority, "broker certificate is signed by an unknown authority", false, err)
	}
	var hostnameError x509.HostnameError
	if errors.As(err, &hostnameError) {
		return newDiagnostic(stage, CodeHostnameMismatch, "broker certificate does not match the configured server name", false, err)
	}
	var invalidCertificate x509.CertificateInvalidError
	if errors.As(err, &invalidCertificate) {
		if invalidCertificate.Reason == x509.Expired {
			now := time.Now()
			if invalidCertificate.Cert != nil && now.Before(invalidCertificate.Cert.NotBefore) {
				return newDiagnostic(stage, CodeCertificateNotYetValid, "broker certificate is not yet valid", false, err)
			}
			return newDiagnostic(stage, CodeCertificateExpired, "broker certificate has expired", false, err)
		}
		return newDiagnostic(stage, CodeBadCertificate, "broker certificate validation failed", false, err)
	}

	var recordHeader tls.RecordHeaderError
	if errors.As(err, &recordHeader) {
		return newDiagnostic(stage, CodeHandshakeFailed, "broker did not complete a valid TLS handshake", false, err)
	}
	if isBadCertificateError(err) {
		return newDiagnostic(stage, CodeBadCertificate, "TLS peer rejected a certificate", false, err)
	}
	var networkError net.Error
	if errors.As(err, &networkError) {
		if networkError.Timeout() {
			return newDiagnostic(stage, CodeTimeout, "TLS network operation timed out", true, err)
		}
		if networkError.Temporary() {
			return newDiagnostic(stage, CodeTemporary, "temporary TLS network failure", true, err)
		}
	}
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrPermission) {
		return newDiagnostic(stage, CodeFileUnavailable, "TLS certificate file is unavailable", false, err)
	}

	message := "TLS operation failed"
	if stage == StageHandshake {
		message = "TLS handshake failed"
	}
	return newDiagnostic(stage, CodeHandshakeFailed, message, false, err)
}

func newDiagnostic(stage DiagnosticStage, code DiagnosticCode, message string, retryable bool, cause error) *DiagnosticError {
	return &DiagnosticError{
		Stage:     stage,
		Code:      code,
		Message:   message,
		Retryable: retryable,
		cause:     cause,
	}
}

func isBadCertificateError(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "bad certificate") ||
		strings.Contains(message, "certificate required") ||
		strings.Contains(message, "unsupported certificate")
}
