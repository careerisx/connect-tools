package session

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	// ErrNotFound indicates that a runtime session ID is absent.
	ErrNotFound = errors.New("session not found")
	// ErrDuplicateID indicates that a runtime session ID is already registered.
	ErrDuplicateID = errors.New("session ID already exists")
	// ErrInvalidSession indicates incomplete or unknown session metadata.
	ErrInvalidSession = errors.New("invalid session")
	// ErrInvalidParent indicates an invalid TCP peer parent relationship.
	ErrInvalidParent = errors.New("invalid parent session")
	// ErrIllegalTransition indicates a state edge outside the explicit lifecycle table.
	ErrIllegalTransition = errors.New("illegal session state transition")
	// ErrSessionActive indicates that an active or transitional session cannot be removed.
	ErrSessionActive = errors.New("active session cannot be removed")
	// ErrSessionHasPeers indicates that a TCP server still owns peer sessions.
	ErrSessionHasPeers = errors.New("session still has peers")
)

var allowedTransitions = map[State]map[State]bool{
	StateStopped:  {StateStarting: true},
	StateStarting: {StateRunning: true, StateError: true, StateStopping: true},
	StateRunning:  {StateStopping: true, StateError: true},
	StateStopping: {StateStopped: true, StateError: true},
	StateError:    {StateStarting: true, StateStopped: true},
}

// Manager owns runtime session metadata. Its zero value is ready to use.
type Manager struct {
	mu       sync.RWMutex
	sessions map[string]Session
	observer Observer
}

// NewManager creates an empty runtime session manager.
func NewManager() *Manager {
	return NewManagerWithObserver(nil)
}

// NewManagerWithObserver creates an empty manager with one process-owned
// observer. The observer is immutable and is called outside the manager lock.
func NewManagerWithObserver(observer Observer) *Manager {
	return &Manager{sessions: make(map[string]Session), observer: observer}
}

// Create adds a session after validating its identity and parent relationship.
func (m *Manager) Create(value Session) (Session, error) {
	m.mu.Lock()
	m.ensureInitialized()
	if err := validateSession(value); err != nil {
		m.mu.Unlock()
		return Session{}, err
	}
	if _, exists := m.sessions[value.ID]; exists {
		m.mu.Unlock()
		return Session{}, fmt.Errorf("%w: %s", ErrDuplicateID, value.ID)
	}
	if err := m.validateParentLocked(value); err != nil {
		m.mu.Unlock()
		return Session{}, err
	}
	value = value.Clone()
	m.sessions[value.ID] = value
	snapshot := value.Clone()
	m.mu.Unlock()
	m.notify(Event{Type: EventCreated, Session: snapshot.Clone(), SessionID: snapshot.ID})
	return snapshot, nil
}

// Get returns a snapshot of one runtime session.
func (m *Manager) Get(id string) (Session, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	value, exists := m.sessions[id]
	if !exists {
		return Session{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return value.Clone(), nil
}

// List returns sessions sorted by creation time and then ID.
func (m *Manager) List() []Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]Session, 0, len(m.sessions))
	for _, value := range m.sessions {
		result = append(result, value.Clone())
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].CreatedAt.Equal(result[right].CreatedAt) {
			return result[left].ID < result[right].ID
		}
		return result[left].CreatedAt.Before(result[right].CreatedAt)
	})
	return result
}

// Transition applies one legal lifecycle transition. Entering Error requires
// a structured failure; later recovery transitions retain it as the most
// recent error for diagnostics.
func (m *Manager) Transition(id string, next State, lastError *Error) (Session, error) {
	m.mu.Lock()
	value, exists := m.sessions[id]
	if !exists {
		m.mu.Unlock()
		return Session{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if !allowedTransitions[value.State][next] {
		m.mu.Unlock()
		return Session{}, fmt.Errorf("%w: %s to %s", ErrIllegalTransition, value.State, next)
	}
	if next == StateError && lastError == nil {
		m.mu.Unlock()
		return Session{}, fmt.Errorf("%w: error state requires last error", ErrInvalidSession)
	}
	value.State = next
	value.UpdatedAt = time.Now()
	if next == StateError {
		copy := Session{LastError: lastError}.Clone()
		value.LastError = copy.LastError
	}
	m.sessions[id] = value
	snapshot := value.Clone()
	m.mu.Unlock()
	m.notify(Event{Type: EventUpdated, Session: snapshot.Clone(), SessionID: snapshot.ID})
	return snapshot, nil
}

// UpdateAddresses stores addresses observed from an active transport while a
// session is starting or running. Callers cannot use it to mutate identity or
// lifecycle state.
func (m *Manager) UpdateAddresses(id, localAddress, remoteAddress string) (Session, error) {
	m.mu.Lock()
	value, exists := m.sessions[id]
	if !exists {
		m.mu.Unlock()
		return Session{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if value.State != StateStarting && value.State != StateRunning {
		m.mu.Unlock()
		return Session{}, fmt.Errorf("%w: addresses require a starting or running session", ErrInvalidSession)
	}
	value.LocalAddress = localAddress
	value.RemoteAddress = remoteAddress
	value.UpdatedAt = time.Now()
	m.sessions[id] = value
	snapshot := value.Clone()
	m.mu.Unlock()
	m.notify(Event{Type: EventUpdated, Session: snapshot.Clone(), SessionID: snapshot.ID})
	return snapshot, nil
}

// Remove deletes a stopped or failed session. A TCP server cannot be removed
// while any peer still refers to it.
func (m *Manager) Remove(id string) (Session, error) {
	m.mu.Lock()
	value, exists := m.sessions[id]
	if !exists {
		m.mu.Unlock()
		return Session{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if value.State != StateStopped && value.State != StateError {
		m.mu.Unlock()
		return Session{}, fmt.Errorf("%w: %s", ErrSessionActive, id)
	}
	if value.Kind == KindTCPServer {
		for _, candidate := range m.sessions {
			if candidate.ParentID == id {
				m.mu.Unlock()
				return Session{}, fmt.Errorf("%w: %s", ErrSessionHasPeers, id)
			}
		}
	}
	delete(m.sessions, id)
	snapshot := value.Clone()
	m.mu.Unlock()
	m.notify(Event{Type: EventRemoved, Session: snapshot.Clone(), SessionID: snapshot.ID})
	return snapshot, nil
}

func (m *Manager) notify(event Event) {
	if m.observer == nil {
		return
	}
	defer func() { _ = recover() }()
	m.observer(Event{Type: event.Type, Session: event.Session.Clone(), SessionID: event.SessionID})
}

func (m *Manager) validateParentLocked(value Session) error {
	if value.Kind == KindTCPPeer {
		if strings.TrimSpace(value.ParentID) == "" {
			return fmt.Errorf("%w: TCP peer requires parent ID", ErrInvalidParent)
		}
		parent, exists := m.sessions[value.ParentID]
		if !exists || parent.Kind != KindTCPServer {
			return fmt.Errorf("%w: parent %q is not a TCP server", ErrInvalidParent, value.ParentID)
		}
		return nil
	}
	if value.ParentID != "" {
		return fmt.Errorf("%w: only TCP peers may have a parent", ErrInvalidParent)
	}
	return nil
}

func (m *Manager) ensureInitialized() {
	if m.sessions == nil {
		m.sessions = make(map[string]Session)
	}
}

func validateSession(value Session) error {
	if strings.TrimSpace(value.ID) == "" || strings.TrimSpace(value.DisplayName) == "" {
		return ErrInvalidSession
	}
	switch value.Kind {
	case KindTCPClient, KindTCPServer, KindTCPPeer, KindMQTTClient:
	default:
		return ErrInvalidSession
	}
	switch value.State {
	case StateStopped, StateStarting, StateRunning, StateStopping, StateError:
	default:
		return ErrInvalidSession
	}
	if value.State == StateError && value.LastError == nil {
		return ErrInvalidSession
	}
	return nil
}
