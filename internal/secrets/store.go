// Package secrets stores profile credentials outside profile JSON documents.
package secrets

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

const (
	// FieldPassword identifies an MQTT password credential.
	FieldPassword = "password"
	// FieldPrivateKeyPassphrase identifies a TLS private-key passphrase.
	FieldPrivateKeyPassphrase = "private-key-passphrase"
)

var (
	// ErrNotFound indicates that a credential reference has no stored value.
	ErrNotFound = errors.New("secret not found")
	// ErrInvalidReference indicates a reference outside the managed profile namespace.
	ErrInvalidReference = errors.New("invalid secret reference")
	// ErrUnavailable indicates that the operating-system credential store is unavailable.
	ErrUnavailable = errors.New("secret store unavailable")
	// ErrWriteStateUnknown indicates that a write succeeded before recovery
	// failed, so the referenced value must be treated as possibly changed.
	ErrWriteStateUnknown = errors.New("secret write state unknown")
)

// MutationError reports a credential mutation whose final state cannot be
// guaranteed. It deliberately omits the credential reference and backend error.
type MutationError struct {
	Operation string
	Err       error
}

// Error implements error without exposing credential data or backend output.
func (e *MutationError) Error() string {
	return e.Operation + " credential: final state is unknown"
}

// Unwrap exposes both the state-unknown sentinel and the sanitized cause.
func (e *MutationError) Unwrap() []error {
	if e.Err == nil {
		return []error{ErrWriteStateUnknown}
	}
	return []error{ErrWriteStateUnknown, e.Err}
}

// Store persists opaque byte values under server-managed references.
type Store interface {
	Put(context.Context, string, []byte) error
	Get(context.Context, string) ([]byte, error)
	Delete(context.Context, string) error
	Persistent() bool
}

// Diagnostic describes the selected credential-store backend without exposing
// backend errors or credential references.
type Diagnostic struct {
	Persistent bool   `json:"persistent"`
	Backend    string `json:"backend"`
	Message    string `json:"message"`
}

// ProfileReference returns the canonical reference for one managed profile field.
func ProfileReference(profileID, field string) (string, error) {
	if !validUUID(profileID) || !validField(field) {
		return "", ErrInvalidReference
	}
	return "profile/" + profileID + "/" + field, nil
}

// ValidateReference verifies the canonical profile/<uuid>/<field> shape.
func ValidateReference(ref string) error {
	parts := strings.Split(ref, "/")
	if len(parts) != 3 || parts[0] != "profile" || !validUUID(parts[1]) || !validField(parts[2]) {
		return ErrInvalidReference
	}
	return nil
}

// ValidateProfileReference verifies that ref belongs to the expected profile and field.
func ValidateProfileReference(ref, profileID, field string) error {
	expected, err := ProfileReference(profileID, field)
	if err != nil || ref != expected {
		return ErrInvalidReference
	}
	return nil
}

func validField(field string) bool {
	return field == FieldPassword || field == FieldPrivateKeyPassphrase
}

func validUUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	for index, character := range value {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			continue
		}
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

func operationError(operation string, sentinel error) error {
	return fmt.Errorf("%s credential: %w", operation, sentinel)
}

func zero(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
