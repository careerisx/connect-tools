package app

import (
	"context"
	"fmt"
	"strings"
	"time"

	"connect-tools/internal/profile"
	"connect-tools/internal/session"
)

// SessionService coordinates profile-backed runtime session metadata. Network
// transports apply Starting/Running/Stopping transitions when their operations
// actually begin and complete.
type SessionService struct {
	profiles profile.Store
	sessions *session.Manager
	now      func() time.Time
}

// NewSessionService creates a runtime session application service.
func NewSessionService(profiles profile.Store, sessions *session.Manager) *SessionService {
	return &SessionService{profiles: profiles, sessions: sessions, now: time.Now}
}

// Create creates a stopped, profile-backed runtime session. It does not claim
// that a network connection has started.
func (s *SessionService) Create(ctx context.Context, profileID, displayName string) (session.Session, error) {
	if err := applicationContextError(ctx); err != nil {
		return session.Session{}, err
	}
	saved, err := s.profiles.Get(ctx, profileID)
	if err != nil {
		return session.Session{}, err
	}
	kind, err := sessionKindForProfile(saved.Type)
	if err != nil {
		return session.Session{}, err
	}
	if strings.TrimSpace(displayName) == "" {
		displayName = saved.Name
	}
	return s.create(session.Session{
		ProfileID:   saved.ID,
		Kind:        kind,
		State:       session.StateStopped,
		DisplayName: displayName,
	})
}

// CreatePeer creates runtime metadata for an accepted TCP server connection.
// The caller supplies addresses observed from the actual socket.
func (s *SessionService) CreatePeer(ctx context.Context, parentID, displayName, localAddress, remoteAddress string) (session.Session, error) {
	if err := applicationContextError(ctx); err != nil {
		return session.Session{}, err
	}
	return s.create(session.Session{
		ParentID:      parentID,
		Kind:          session.KindTCPPeer,
		State:         session.StateRunning,
		DisplayName:   displayName,
		LocalAddress:  localAddress,
		RemoteAddress: remoteAddress,
	})
}

// Get returns one runtime session snapshot.
func (s *SessionService) Get(ctx context.Context, id string) (session.Session, error) {
	if err := applicationContextError(ctx); err != nil {
		return session.Session{}, err
	}
	return s.sessions.Get(id)
}

// List returns all runtime sessions in deterministic order.
func (s *SessionService) List(ctx context.Context) ([]session.Session, error) {
	if err := applicationContextError(ctx); err != nil {
		return nil, err
	}
	return s.sessions.List(), nil
}

// GetProfile returns the saved profile used to route a session start.
func (s *SessionService) GetProfile(ctx context.Context, id string) (profile.Profile, error) {
	if err := applicationContextError(ctx); err != nil {
		return profile.Profile{}, err
	}
	return s.profiles.Get(ctx, id)
}

// Transition applies a transport-confirmed lifecycle state change.
func (s *SessionService) Transition(ctx context.Context, id string, next session.State, lastError *session.Error) (session.Session, error) {
	if err := applicationContextError(ctx); err != nil {
		return session.Session{}, err
	}
	return s.sessions.Transition(id, next, lastError)
}

// UpdateAddresses records addresses obtained from a starting or running
// transport without allowing arbitrary session mutation.
func (s *SessionService) UpdateAddresses(ctx context.Context, id, localAddress, remoteAddress string) (session.Session, error) {
	if err := applicationContextError(ctx); err != nil {
		return session.Session{}, err
	}
	return s.sessions.UpdateAddresses(id, localAddress, remoteAddress)
}

// Remove deletes stopped or failed runtime session metadata.
func (s *SessionService) Remove(ctx context.Context, id string) (session.Session, error) {
	if err := applicationContextError(ctx); err != nil {
		return session.Session{}, err
	}
	return s.sessions.Remove(id)
}

func (s *SessionService) create(input session.Session) (session.Session, error) {
	id, err := newUUID()
	if err != nil {
		return session.Session{}, err
	}
	now := s.now()
	input.ID = id
	input.CreatedAt = now
	input.UpdatedAt = now
	return s.sessions.Create(input)
}

func sessionKindForProfile(profileType profile.Type) (session.Kind, error) {
	switch profileType {
	case profile.TypeTCPClient:
		return session.KindTCPClient, nil
	case profile.TypeTCPServer:
		return session.KindTCPServer, nil
	case profile.TypeMQTTClient:
		return session.KindMQTTClient, nil
	default:
		return "", fmt.Errorf("unsupported profile type %q", profileType)
	}
}

func applicationContextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}
