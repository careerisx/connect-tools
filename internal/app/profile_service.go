package app

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"time"

	"connect-tools/internal/profile"
	"connect-tools/internal/secrets"
)

var (
	// ErrInvalidSecretUpdate indicates an inconsistent secret update action.
	ErrInvalidSecretUpdate = errors.New("invalid profile secret update")
	// ErrSecretStoreRequired indicates that a requested credential operation has no store.
	ErrSecretStoreRequired = errors.New("profile secret store is required")
	// ErrSecretRollback indicates that profile persistence failed and credential rollback was incomplete.
	ErrSecretRollback = errors.New("profile secret rollback failed")
	// ErrSecretCleanup indicates that the profile change succeeded but obsolete credentials could not all be removed.
	ErrSecretCleanup = errors.New("profile secret cleanup failed")
)

// SecretUpdateAction selects how a profile credential changes.
type SecretUpdateAction uint8

const (
	// SecretKeep leaves the existing credential reference and value unchanged.
	SecretKeep SecretUpdateAction = iota
	// SecretSet stores Value under the profile's managed reference.
	SecretSet
	// SecretDelete removes the credential and clears its profile reference.
	SecretDelete
)

// SecretUpdate distinguishes an unchanged, replaced, or explicitly deleted credential.
// Value is read only when Action is SecretSet and is never retained by the service.
type SecretUpdate struct {
	Action SecretUpdateAction
	Value  []byte
}

// ProfileSecretUpdates contains credentials that are deliberately excluded
// from the persisted Profile model.
type ProfileSecretUpdates struct {
	Password             SecretUpdate
	PrivateKeyPassphrase SecretUpdate
}

// ProfileMutationError reports a partial credential transaction without
// including credential values or references.
type ProfileMutationError struct {
	Operation        string
	ProfilePersisted bool
	Err              error
}

// Error implements error.
func (e *ProfileMutationError) Error() string {
	if e.ProfilePersisted {
		return e.Operation + ": profile change was persisted but credential cleanup was incomplete"
	}
	return e.Operation + ": profile change was not persisted and credential rollback was incomplete"
}

// Unwrap exposes the stable cleanup or rollback sentinel.
func (e *ProfileMutationError) Unwrap() error { return e.Err }

// ProfileService coordinates server-managed profile identity, timestamps, and
// credential references.
type ProfileService struct {
	store   profile.Store
	secrets secrets.Store
	now     func() time.Time
	observe func(ProfileEvent)
	mu      sync.Mutex
}

// NewProfileService creates a profile application service without managed
// credential mutation support. Existing TCP behavior remains unchanged.
func NewProfileService(store profile.Store) *ProfileService {
	return NewProfileServiceWithSecrets(store, nil)
}

// NewProfileServiceWithSecrets creates a profile service backed by secretStore.
func NewProfileServiceWithSecrets(store profile.Store, secretStore secrets.Store) *ProfileService {
	return NewProfileServiceWithObserver(store, secretStore, nil)
}

// NewProfileServiceWithObserver creates a profile service with a single
// process-owned observer. Notifications run after the service lock is released.
func NewProfileServiceWithObserver(store profile.Store, secretStore secrets.Store, observer func(ProfileEvent)) *ProfileService {
	return &ProfileService{store: store, secrets: secretStore, now: time.Now, observe: observer}
}

// Create assigns identity and timestamps, validates the profile and persists it.
// MQTT callers that need to set credentials should use CreateWithSecrets.
func (s *ProfileService) Create(ctx context.Context, input profile.Profile) (profile.Profile, error) {
	return s.CreateWithSecrets(ctx, input, ProfileSecretUpdates{})
}

// CreateWithSecrets creates a profile and stores supplied credentials under
// references derived from the new server-generated profile ID.
func (s *ProfileService) CreateWithSecrets(ctx context.Context, input profile.Profile, updates ProfileSecretUpdates) (profile.Profile, error) {
	s.mu.Lock()
	var event *ProfileEvent
	defer func() {
		s.mu.Unlock()
		s.notify(event)
	}()

	updates, clearUpdates, err := cloneSecretUpdates(updates)
	if err != nil {
		return profile.Profile{}, err
	}
	defer clearUpdates()

	id, err := newUUID()
	if err != nil {
		return profile.Profile{}, err
	}
	now := s.now()
	input = input.Clone()
	input.ID = id
	input.CreatedAt = now
	input.UpdatedAt = now
	if err := prepareCreatedReferences(&input, updates); err != nil {
		return profile.Profile{}, err
	}
	if err := input.Validate(); err != nil {
		return profile.Profile{}, err
	}

	writes, err := s.applySecretSets(ctx, input.ID, updates)
	if err != nil {
		return profile.Profile{}, err
	}
	defer clearWriteSnapshots(writes)
	if err := s.store.Put(ctx, input); err != nil {
		if rollbackErr := s.rollbackWrites(cleanupContext(ctx), writes); rollbackErr != nil {
			return profile.Profile{}, &ProfileMutationError{Operation: "create profile", Err: ErrSecretRollback}
		}
		return profile.Profile{}, err
	}
	event = &ProfileEvent{Type: ProfileEventCreated, ProfileID: input.ID}
	return input.Clone(), nil
}

// Update fully replaces mutable profile fields while preserving its ID and
// creation timestamp. Existing credential references are retained.
func (s *ProfileService) Update(ctx context.Context, input profile.Profile) (profile.Profile, error) {
	return s.UpdateWithSecrets(ctx, input, ProfileSecretUpdates{})
}

// UpdateWithSecrets replaces mutable profile fields and atomically coordinates
// credential writes with the persisted reference changes.
func (s *ProfileService) UpdateWithSecrets(ctx context.Context, input profile.Profile, updates ProfileSecretUpdates) (profile.Profile, error) {
	s.mu.Lock()
	var event *ProfileEvent
	defer func() {
		s.mu.Unlock()
		s.notify(event)
	}()

	updates, clearUpdates, err := cloneSecretUpdates(updates)
	if err != nil {
		return profile.Profile{}, err
	}
	defer clearUpdates()

	existing, err := s.store.Get(ctx, input.ID)
	if err != nil {
		return profile.Profile{}, err
	}
	input = input.Clone()
	input.ID = existing.ID
	input.CreatedAt = existing.CreatedAt
	input.UpdatedAt = s.now()
	cleanupRefs, err := prepareUpdatedReferences(&input, existing, updates)
	if err != nil {
		return profile.Profile{}, err
	}
	if err := input.Validate(); err != nil {
		return profile.Profile{}, err
	}

	writes, err := s.applySecretSets(ctx, input.ID, updates)
	if err != nil {
		return profile.Profile{}, err
	}
	defer clearWriteSnapshots(writes)
	if err := s.store.Put(ctx, input); err != nil {
		if rollbackErr := s.rollbackWrites(cleanupContext(ctx), writes); rollbackErr != nil {
			return profile.Profile{}, &ProfileMutationError{Operation: "update profile", Err: ErrSecretRollback}
		}
		return profile.Profile{}, err
	}
	event = &ProfileEvent{Type: ProfileEventUpdated, ProfileID: input.ID}
	if err := s.cleanupReferences(cleanupContext(ctx), cleanupRefs); err != nil {
		return input.Clone(), &ProfileMutationError{Operation: "update profile", ProfilePersisted: true, Err: ErrSecretCleanup}
	}
	return input.Clone(), nil
}

// Delete removes a saved profile, then best-effort removes credentials that are
// no longer referenced. A cleanup error means the profile is already deleted.
func (s *ProfileService) Delete(ctx context.Context, id string) error {
	s.mu.Lock()
	var event *ProfileEvent
	defer func() {
		s.mu.Unlock()
		s.notify(event)
	}()

	existing, err := s.store.Get(ctx, id)
	if err != nil {
		return err
	}
	refs := managedReferences(existing)
	if err := s.store.Delete(ctx, id); err != nil {
		return err
	}
	event = &ProfileEvent{Type: ProfileEventDeleted, ProfileID: existing.ID}
	if err := s.cleanupReferences(cleanupContext(ctx), refs); err != nil {
		return &ProfileMutationError{Operation: "delete profile", ProfilePersisted: true, Err: ErrSecretCleanup}
	}
	return nil
}

func (s *ProfileService) notify(event *ProfileEvent) {
	if event == nil || s.observe == nil {
		return
	}
	defer func() { _ = recover() }()
	s.observe(*event)
}

// Get returns one saved profile.
func (s *ProfileService) Get(ctx context.Context, id string) (profile.Profile, error) {
	return s.store.Get(ctx, id)
}

// List returns all saved profiles in deterministic order.
func (s *ProfileService) List(ctx context.Context) ([]profile.Profile, error) {
	return s.store.List(ctx)
}

type secretWrite struct {
	ref      string
	previous []byte
	existed  bool
}

func prepareCreatedReferences(input *profile.Profile, updates ProfileSecretUpdates) error {
	if input.MQTT == nil {
		if updates.Password.Action != SecretKeep || updates.PrivateKeyPassphrase.Action != SecretKeep {
			return ErrInvalidSecretUpdate
		}
		return nil
	}
	if input.MQTT.PasswordRef != "" || input.MQTT.TLS.PrivateKeyPassphraseRef != "" {
		return ErrInvalidSecretUpdate
	}
	var err error
	if updates.Password.Action == SecretSet {
		input.MQTT.PasswordRef, err = secrets.ProfileReference(input.ID, secrets.FieldPassword)
		if err != nil {
			return err
		}
	}
	if updates.PrivateKeyPassphrase.Action == SecretSet {
		input.MQTT.TLS.PrivateKeyPassphraseRef, err = secrets.ProfileReference(input.ID, secrets.FieldPrivateKeyPassphrase)
		if err != nil {
			return err
		}
	}
	return nil
}

func prepareUpdatedReferences(input *profile.Profile, existing profile.Profile, updates ProfileSecretUpdates) ([]string, error) {
	oldRefs := managedReferences(existing)
	if input.MQTT == nil {
		if updates.Password.Action == SecretSet || updates.PrivateKeyPassphrase.Action == SecretSet {
			return nil, ErrInvalidSecretUpdate
		}
		return oldRefs, nil
	}
	var oldPasswordRef, oldPassphraseRef string
	if existing.MQTT != nil {
		oldPasswordRef = existing.MQTT.PasswordRef
		oldPassphraseRef = existing.MQTT.TLS.PrivateKeyPassphraseRef
	}
	if input.MQTT.PasswordRef != "" && input.MQTT.PasswordRef != oldPasswordRef {
		return nil, ErrInvalidSecretUpdate
	}
	if input.MQTT.TLS.PrivateKeyPassphraseRef != "" && input.MQTT.TLS.PrivateKeyPassphraseRef != oldPassphraseRef {
		return nil, ErrInvalidSecretUpdate
	}
	cleanup := make([]string, 0, 2)
	passwordRef, removePassword, err := updatedReference(input.ID, secrets.FieldPassword, oldPasswordRef, updates.Password.Action)
	if err != nil {
		return nil, err
	}
	input.MQTT.PasswordRef = passwordRef
	if removePassword != "" {
		cleanup = append(cleanup, removePassword)
	}
	passphraseRef, removePassphrase, err := updatedReference(input.ID, secrets.FieldPrivateKeyPassphrase, oldPassphraseRef, updates.PrivateKeyPassphrase.Action)
	if err != nil {
		return nil, err
	}
	input.MQTT.TLS.PrivateKeyPassphraseRef = passphraseRef
	if removePassphrase != "" {
		cleanup = append(cleanup, removePassphrase)
	}
	return cleanup, nil
}

func updatedReference(profileID, field, existingRef string, action SecretUpdateAction) (string, string, error) {
	switch action {
	case SecretKeep:
		return existingRef, "", nil
	case SecretSet:
		ref, err := secrets.ProfileReference(profileID, field)
		return ref, "", err
	case SecretDelete:
		return "", existingRef, nil
	default:
		return "", "", ErrInvalidSecretUpdate
	}
}

func (s *ProfileService) applySecretSets(ctx context.Context, profileID string, updates ProfileSecretUpdates) ([]secretWrite, error) {
	requested := []struct {
		field  string
		update SecretUpdate
	}{
		{field: secrets.FieldPassword, update: updates.Password},
		{field: secrets.FieldPrivateKeyPassphrase, update: updates.PrivateKeyPassphrase},
	}
	writes := make([]secretWrite, 0, 2)
	for _, item := range requested {
		if item.update.Action != SecretSet {
			continue
		}
		if s.secrets == nil {
			_ = s.rollbackWrites(cleanupContext(ctx), writes)
			clearWriteSnapshots(writes)
			return nil, ErrSecretStoreRequired
		}
		ref, err := secrets.ProfileReference(profileID, item.field)
		if err != nil {
			return nil, err
		}
		write := secretWrite{ref: ref}
		previous, getErr := s.secrets.Get(ctx, ref)
		switch {
		case getErr == nil:
			write.previous = previous
			write.existed = true
		case errors.Is(getErr, secrets.ErrNotFound):
		case getErr != nil:
			if rollbackErr := s.rollbackWrites(cleanupContext(ctx), writes); rollbackErr != nil {
				clearWriteSnapshots(writes)
				return nil, &ProfileMutationError{Operation: "prepare profile credentials", Err: ErrSecretRollback}
			}
			clearWriteSnapshots(writes)
			return nil, getErr
		}
		if err := s.secrets.Put(ctx, ref, item.update.Value); err != nil {
			if errors.Is(err, secrets.ErrWriteStateUnknown) {
				writes = append(writes, write)
				_ = s.rollbackWrites(cleanupContext(ctx), writes)
				clearWriteSnapshots(writes)
				return nil, &ProfileMutationError{
					Operation: "store profile credentials",
					Err:       errors.Join(ErrSecretRollback, err),
				}
			}
			zeroBytes(write.previous)
			if rollbackErr := s.rollbackWrites(cleanupContext(ctx), writes); rollbackErr != nil {
				clearWriteSnapshots(writes)
				return nil, &ProfileMutationError{Operation: "store profile credentials", Err: ErrSecretRollback}
			}
			clearWriteSnapshots(writes)
			return nil, err
		}
		writes = append(writes, write)
	}
	return writes, nil
}

func (s *ProfileService) rollbackWrites(ctx context.Context, writes []secretWrite) error {
	if s.secrets == nil {
		if len(writes) == 0 {
			return nil
		}
		return ErrSecretRollback
	}
	var rollbackFailed bool
	for index := len(writes) - 1; index >= 0; index-- {
		write := writes[index]
		if write.existed {
			if err := s.secrets.Put(ctx, write.ref, write.previous); err != nil {
				rollbackFailed = true
			}
			continue
		}
		if err := s.secrets.Delete(ctx, write.ref); err != nil && !errors.Is(err, secrets.ErrNotFound) {
			rollbackFailed = true
		}
	}
	if rollbackFailed {
		return ErrSecretRollback
	}
	return nil
}

func (s *ProfileService) cleanupReferences(ctx context.Context, refs []string) error {
	if len(refs) == 0 {
		return nil
	}
	if s.secrets == nil {
		return ErrSecretStoreRequired
	}
	var failed bool
	for _, ref := range refs {
		if ref == "" {
			continue
		}
		if err := s.secrets.Delete(ctx, ref); err != nil && !errors.Is(err, secrets.ErrNotFound) {
			failed = true
		}
	}
	if failed {
		return ErrSecretCleanup
	}
	return nil
}

func managedReferences(value profile.Profile) []string {
	if value.MQTT == nil {
		return nil
	}
	refs := make([]string, 0, 2)
	if value.MQTT.PasswordRef != "" {
		refs = append(refs, value.MQTT.PasswordRef)
	}
	if value.MQTT.TLS.PrivateKeyPassphraseRef != "" {
		refs = append(refs, value.MQTT.TLS.PrivateKeyPassphraseRef)
	}
	return refs
}

func cloneSecretUpdates(updates ProfileSecretUpdates) (ProfileSecretUpdates, func(), error) {
	if err := validateSecretUpdate(updates.Password); err != nil {
		return ProfileSecretUpdates{}, func() {}, err
	}
	if err := validateSecretUpdate(updates.PrivateKeyPassphrase); err != nil {
		return ProfileSecretUpdates{}, func() {}, err
	}
	clone := updates
	clone.Password.Value = append([]byte(nil), updates.Password.Value...)
	clone.PrivateKeyPassphrase.Value = append([]byte(nil), updates.PrivateKeyPassphrase.Value...)
	return clone, func() {
		zeroBytes(clone.Password.Value)
		zeroBytes(clone.PrivateKeyPassphrase.Value)
	}, nil
}

func validateSecretUpdate(update SecretUpdate) error {
	if update.Action > SecretDelete {
		return ErrInvalidSecretUpdate
	}
	if update.Action != SecretSet && len(update.Value) != 0 {
		return ErrInvalidSecretUpdate
	}
	return nil
}

func clearWriteSnapshots(writes []secretWrite) {
	for index := range writes {
		zeroBytes(writes[index].previous)
	}
}

func zeroBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

func cleanupContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return context.WithoutCancel(ctx)
}

func newUUID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate UUID: %w", err)
	}
	raw[6] = raw[6]&0x0f | 0x40
	raw[8] = raw[8]&0x3f | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:16]), nil
}
