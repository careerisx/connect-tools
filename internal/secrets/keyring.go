package secrets

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"

	keyring "github.com/zalando/go-keyring"
)

const (
	keyringService = "connect-tools"
	valuePrefix    = "v1:"
)

// KeyringStore persists credentials in the operating-system credential store.
type KeyringStore struct{}

// NewKeyring creates a keyring-backed store. Call ProbeKeyring or Open before
// relying on platform availability.
func NewKeyring() *KeyringStore { return &KeyringStore{} }

// Open probes the operating-system credential store and falls back to an
// in-memory store with a non-persistent diagnostic when it is unavailable.
func Open(ctx context.Context) (Store, Diagnostic) {
	store := NewKeyring()
	if err := ProbeKeyring(ctx); err == nil {
		return store, Diagnostic{
			Persistent: true,
			Backend:    "keyring",
			Message:    "credentials are stored in the operating-system keyring",
		}
	}
	return NewMemory(), Diagnostic{
		Persistent: false,
		Backend:    "memory",
		Message:    "operating-system keyring unavailable; credentials will be lost when the process exits",
	}
}

// ProbeKeyring verifies keyring write, read, and delete operations using a
// random transient key that cannot overlap the managed profile namespace.
func ProbeKeyring(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return operationError("probe", ErrUnavailable)
	}
	probeRef := "probe/" + hex.EncodeToString(random[:])
	probeValue := valuePrefix + base64.StdEncoding.EncodeToString(random[:])
	if err := contextError(ctx); err != nil {
		return err
	}
	setErr := keyring.Set(keyringService, probeRef, probeValue)
	if err := contextError(ctx); err != nil {
		cleanupProbe(ctx, probeRef)
		return err
	}
	if setErr != nil {
		cleanupProbe(ctx, probeRef)
		return operationError("probe", ErrUnavailable)
	}
	defer cleanupProbe(ctx, probeRef)
	if err := contextError(ctx); err != nil {
		return err
	}
	stored, err := keyring.Get(keyringService, probeRef)
	if contextErr := contextError(ctx); contextErr != nil {
		return contextErr
	}
	if err != nil || stored != probeValue {
		return operationError("probe", ErrUnavailable)
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	deleteErr := keyring.Delete(keyringService, probeRef)
	if err := contextError(ctx); err != nil {
		return err
	}
	if deleteErr != nil {
		return operationError("probe", ErrUnavailable)
	}
	return nil
}

// Put encodes arbitrary bytes and stores them in the operating-system keyring.
func (*KeyringStore) Put(ctx context.Context, ref string, value []byte) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := ValidateReference(ref); err != nil {
		return err
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	previous, previousErr := keyring.Get(keyringService, ref)
	if err := contextError(ctx); err != nil {
		return err
	}
	if previousErr != nil && !errors.Is(previousErr, keyring.ErrNotFound) {
		return operationError("store", ErrUnavailable)
	}
	encoded := valuePrefix + base64.StdEncoding.EncodeToString(value)
	if err := contextError(ctx); err != nil {
		return err
	}
	setErr := keyring.Set(keyringService, ref, encoded)
	ctxErr := contextError(ctx)
	if setErr != nil {
		return &MutationError{Operation: "store", Err: ErrUnavailable}
	}
	if ctxErr != nil {
		recoveryCtx := context.WithoutCancel(nonNilContext(ctx))
		if previousErr == nil {
			if err := contextError(recoveryCtx); err != nil {
				return &MutationError{Operation: "restore", Err: ErrUnavailable}
			}
			if restoreErr := keyring.Set(keyringService, ref, previous); restoreErr != nil {
				return &MutationError{Operation: "restore", Err: ErrUnavailable}
			}
			if err := contextError(recoveryCtx); err != nil {
				return &MutationError{Operation: "restore", Err: ErrUnavailable}
			}
		} else {
			if err := contextError(recoveryCtx); err != nil {
				return &MutationError{Operation: "restore", Err: ErrUnavailable}
			}
			deleteErr := keyring.Delete(keyringService, ref)
			if err := contextError(recoveryCtx); err != nil {
				return &MutationError{Operation: "restore", Err: ErrUnavailable}
			}
			if deleteErr != nil && !errors.Is(deleteErr, keyring.ErrNotFound) {
				return &MutationError{Operation: "restore", Err: ErrUnavailable}
			}
		}
		return ctxErr
	}
	return nil
}

// Get retrieves and strictly decodes one credential from the keyring.
func (*KeyringStore) Get(ctx context.Context, ref string) ([]byte, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if err := ValidateReference(ref); err != nil {
		return nil, err
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	encoded, err := keyring.Get(keyringService, ref)
	ctxErr := contextError(ctx)
	if ctxErr != nil {
		return nil, ctxErr
	}
	if errors.Is(err, keyring.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, operationError("read", ErrUnavailable)
	}
	if !strings.HasPrefix(encoded, valuePrefix) {
		return nil, operationError("decode", ErrUnavailable)
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(strings.TrimPrefix(encoded, valuePrefix))
	if err != nil {
		return nil, operationError("decode", ErrUnavailable)
	}
	return decoded, nil
}

// Delete removes one credential from the keyring.
func (*KeyringStore) Delete(ctx context.Context, ref string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := ValidateReference(ref); err != nil {
		return err
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	err := keyring.Delete(keyringService, ref)
	ctxErr := contextError(ctx)
	if ctxErr != nil {
		return ctxErr
	}
	if errors.Is(err, keyring.ErrNotFound) {
		return ErrNotFound
	}
	if err != nil {
		return operationError("delete", ErrUnavailable)
	}
	return nil
}

// Persistent reports true for the operating-system keyring backend.
func (*KeyringStore) Persistent() bool { return true }

func nonNilContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func cleanupProbe(ctx context.Context, ref string) {
	cleanupCtx := context.WithoutCancel(nonNilContext(ctx))
	if contextError(cleanupCtx) != nil {
		return
	}
	_ = keyring.Delete(keyringService, ref)
	_ = contextError(cleanupCtx)
}
