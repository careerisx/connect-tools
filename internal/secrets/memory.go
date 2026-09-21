package secrets

import (
	"context"
	"sync"
)

// MemoryStore is a process-local credential store used when the platform
// keyring cannot be used. Its contents are lost when the process exits.
type MemoryStore struct {
	mu     sync.RWMutex
	values map[string][]byte
}

// NewMemory creates an empty non-persistent credential store.
func NewMemory() *MemoryStore {
	return &MemoryStore{values: make(map[string][]byte)}
}

// Put copies value into the store and clears any replaced internal buffer.
func (s *MemoryStore) Put(ctx context.Context, ref string, value []byte) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := ValidateReference(ref); err != nil {
		return err
	}
	copyValue := append([]byte(nil), value...)

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := contextError(ctx); err != nil {
		zero(copyValue)
		return err
	}
	if previous, exists := s.values[ref]; exists {
		zero(previous)
	}
	if s.values == nil {
		s.values = make(map[string][]byte)
	}
	s.values[ref] = copyValue
	return nil
}

// Get returns a caller-owned copy of the stored value.
func (s *MemoryStore) Get(ctx context.Context, ref string) ([]byte, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if err := ValidateReference(ref); err != nil {
		return nil, err
	}
	s.mu.RLock()
	value, exists := s.values[ref]
	if exists {
		value = append([]byte(nil), value...)
	}
	s.mu.RUnlock()
	if err := contextError(ctx); err != nil {
		zero(value)
		return nil, err
	}
	if !exists {
		return nil, ErrNotFound
	}
	return value, nil
}

// Delete clears and removes a stored value.
func (s *MemoryStore) Delete(ctx context.Context, ref string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := ValidateReference(ref); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := contextError(ctx); err != nil {
		return err
	}
	value, exists := s.values[ref]
	if !exists {
		return ErrNotFound
	}
	zero(value)
	delete(s.values, ref)
	return nil
}

// Persistent reports false because values exist only for this process.
func (*MemoryStore) Persistent() bool { return false }
