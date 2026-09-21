package profile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"

	"connect-tools/internal/config"
)

const documentVersion = 1

var (
	// ErrNotFound indicates that a profile ID is absent.
	ErrNotFound = errors.New("profile not found")
	// ErrDuplicateName indicates a case-insensitive profile name collision.
	ErrDuplicateName = errors.New("profile name already exists")
	// ErrCorruptStore indicates malformed or internally invalid persisted data.
	ErrCorruptStore = errors.New("profile store is corrupt")
	// ErrUnsupportedVersion indicates a profile document requiring migration.
	ErrUnsupportedVersion = errors.New("unsupported profile store version")
)

var fileStoreLockRegistry = struct {
	sync.Mutex
	byPath map[string]*sync.Mutex
}{
	byPath: make(map[string]*sync.Mutex),
}

// Store persists connection profiles.
type Store interface {
	List(context.Context) ([]Profile, error)
	Get(context.Context, string) (Profile, error)
	Put(context.Context, Profile) error
	Delete(context.Context, string) error
}

// FileStore persists a versioned profile document. All instances targeting the
// same normalized absolute path share a process-wide lock, while different
// paths can proceed independently. Successful writes replace the document
// atomically.
type FileStore struct {
	path    string
	mu      *sync.Mutex
	pathErr error
}

type document struct {
	Version  int       `json:"version"`
	Profiles []Profile `json:"profiles"`
}

// NewFileStore creates a profile store backed by path.
func NewFileStore(path string) *FileStore {
	normalizedPath, key, err := normalizeStorePath(path)
	if err != nil {
		return &FileStore{path: path, pathErr: err}
	}
	return &FileStore{
		path: normalizedPath,
		mu:   registeredFileStoreLock(key),
	}
}

// List returns all profiles sorted by case-insensitive name and then ID.
func (s *FileStore) List(ctx context.Context) ([]Profile, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if s.pathErr != nil {
		return nil, s.pathErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	doc, err := s.readDocument()
	if err != nil {
		return nil, err
	}
	return cloneAndSort(doc.Profiles), nil
}

// Get returns the profile identified by id.
func (s *FileStore) Get(ctx context.Context, id string) (Profile, error) {
	if err := contextError(ctx); err != nil {
		return Profile{}, err
	}
	if s.pathErr != nil {
		return Profile{}, s.pathErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	doc, err := s.readDocument()
	if err != nil {
		return Profile{}, err
	}
	for _, candidate := range doc.Profiles {
		if candidate.ID == id {
			return candidate.Clone(), nil
		}
	}
	return Profile{}, fmt.Errorf("%w: %s", ErrNotFound, id)
}

// Put inserts or fully replaces a profile with the same ID. Names are unique
// under Unicode case folding.
func (s *FileStore) Put(ctx context.Context, value Profile) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := value.Validate(); err != nil {
		return err
	}
	if s.pathErr != nil {
		return s.pathErr
	}
	value = value.Clone()

	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.readDocument()
	if err != nil {
		return err
	}
	for _, existing := range doc.Profiles {
		if existing.ID != value.ID && strings.EqualFold(existing.Name, value.Name) {
			return fmt.Errorf("%w: %s", ErrDuplicateName, value.Name)
		}
	}

	replaced := false
	for index := range doc.Profiles {
		if doc.Profiles[index].ID == value.ID {
			doc.Profiles[index] = value
			replaced = true
			break
		}
	}
	if !replaced {
		doc.Profiles = append(doc.Profiles, value)
	}
	return s.writeDocument(ctx, doc)
}

// Delete removes the profile identified by id.
func (s *FileStore) Delete(ctx context.Context, id string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if s.pathErr != nil {
		return s.pathErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.readDocument()
	if err != nil {
		return err
	}
	for index := range doc.Profiles {
		if doc.Profiles[index].ID != id {
			continue
		}
		doc.Profiles = append(doc.Profiles[:index], doc.Profiles[index+1:]...)
		return s.writeDocument(ctx, doc)
	}
	return fmt.Errorf("%w: %s", ErrNotFound, id)
}

func (s *FileStore) readDocument() (document, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return document{Version: documentVersion}, nil
	}
	if err != nil {
		return document{}, fmt.Errorf("read profile store %q: %w", s.path, err)
	}

	var doc document
	if err := json.Unmarshal(data, &doc); err != nil {
		return document{}, fmt.Errorf("%w: decode %q: %v", ErrCorruptStore, s.path, err)
	}
	if doc.Version != documentVersion {
		return document{}, fmt.Errorf("%w: got %d, want %d", ErrUnsupportedVersion, doc.Version, documentVersion)
	}
	if err := validateDocument(doc); err != nil {
		return document{}, fmt.Errorf("%w: %v", ErrCorruptStore, err)
	}
	return doc, nil
}

func (s *FileStore) writeDocument(ctx context.Context, doc document) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	doc.Version = documentVersion
	doc.Profiles = cloneAndSort(doc.Profiles)
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return fmt.Errorf("encode profile store: %w", err)
	}
	data = append(data, '\n')
	if err := config.WritePrivateFile(s.path, data); err != nil {
		return fmt.Errorf("write profile store: %w", err)
	}
	return nil
}

func validateDocument(doc document) error {
	ids := make(map[string]struct{}, len(doc.Profiles))
	for index, value := range doc.Profiles {
		if err := value.Validate(); err != nil {
			return err
		}
		if _, exists := ids[value.ID]; exists {
			return fmt.Errorf("duplicate profile ID %q", value.ID)
		}
		ids[value.ID] = struct{}{}
		for previous := 0; previous < index; previous++ {
			if strings.EqualFold(doc.Profiles[previous].Name, value.Name) {
				return fmt.Errorf("duplicate profile name %q", value.Name)
			}
		}
	}
	return nil
}

func cloneAndSort(values []Profile) []Profile {
	result := make([]Profile, len(values))
	for index, value := range values {
		result[index] = value.Clone()
	}
	sort.Slice(result, func(left, right int) bool {
		leftName := strings.ToLower(result[left].Name)
		rightName := strings.ToLower(result[right].Name)
		if leftName == rightName {
			return result[left].ID < result[right].ID
		}
		return leftName < rightName
	})
	return result
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

func normalizeStorePath(path string) (normalizedPath, key string, err error) {
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return "", "", fmt.Errorf("resolve profile store path %q: %w", path, err)
	}
	normalizedPath = filepath.Clean(absolutePath)
	key = normalizedPath
	if runtime.GOOS == "windows" {
		key = strings.ToLower(key)
	}
	return normalizedPath, key, nil
}

func registeredFileStoreLock(key string) *sync.Mutex {
	fileStoreLockRegistry.Lock()
	defer fileStoreLockRegistry.Unlock()
	if lock, exists := fileStoreLockRegistry.byPath[key]; exists {
		return lock
	}

	lock := new(sync.Mutex)
	fileStoreLockRegistry.byPath[key] = lock
	// Profile paths are few and process-scoped. Keeping their locks for the
	// process lifetime avoids unsafe eviction races that could create two live
	// locks for one file while an older FileStore still exists.
	return lock
}
