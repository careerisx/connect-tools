// Package session defines transport-neutral runtime session metadata and
// lifecycle state.
package session

import "time"

// Kind identifies a runtime session transport or child connection.
type Kind string

const (
	// KindTCPClient identifies a TCP client session.
	KindTCPClient Kind = "tcp-client"
	// KindTCPServer identifies a TCP listener session.
	KindTCPServer Kind = "tcp-server"
	// KindTCPPeer identifies one accepted child of a TCP server.
	KindTCPPeer Kind = "tcp-peer"
	// KindMQTTClient identifies an MQTT client session.
	KindMQTTClient Kind = "mqtt-client"
)

// State describes a runtime session lifecycle state.
type State string

const (
	// StateStopped indicates no transport resources are active.
	StateStopped State = "stopped"
	// StateStarting indicates transport startup is in progress.
	StateStarting State = "starting"
	// StateRunning indicates the transport is accepting work.
	StateRunning State = "running"
	// StateStopping indicates transport shutdown is in progress.
	StateStopping State = "stopping"
	// StateError indicates the latest lifecycle operation failed.
	StateError State = "error"
)

// Error describes the most recent structured session failure.
type Error struct {
	Code      string            `json:"code"`
	Message   string            `json:"message"`
	Operation string            `json:"operation"`
	Retryable bool              `json:"retryable"`
	Timestamp time.Time         `json:"timestamp"`
	Details   map[string]string `json:"details,omitempty"`
}

// Session is runtime-only metadata shared by transports and API surfaces.
type Session struct {
	ID            string    `json:"id"`
	ProfileID     string    `json:"profile_id,omitempty"`
	ParentID      string    `json:"parent_id,omitempty"`
	Kind          Kind      `json:"kind"`
	State         State     `json:"state"`
	DisplayName   string    `json:"display_name"`
	LocalAddress  string    `json:"local_address,omitempty"`
	RemoteAddress string    `json:"remote_address,omitempty"`
	LastError     *Error    `json:"last_error,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// EventType identifies a runtime-session metadata change.
type EventType uint8

const (
	// EventCreated indicates that a session was added.
	EventCreated EventType = iota + 1
	// EventUpdated indicates that a session snapshot changed.
	EventUpdated
	// EventRemoved indicates that a session was removed.
	EventRemoved
)

// Event is the immutable session snapshot delivered to an Observer.
type Event struct {
	Type      EventType
	Session   Session
	SessionID string
}

// Observer receives session events after the manager has released its lock.
type Observer func(Event)

// Clone returns a deep copy of s.
func (s Session) Clone() Session {
	clone := s
	if s.LastError != nil {
		lastError := *s.LastError
		if s.LastError.Details != nil {
			lastError.Details = make(map[string]string, len(s.LastError.Details))
			for key, value := range s.LastError.Details {
				lastError.Details[key] = value
			}
		}
		clone.LastError = &lastError
	}
	return clone
}
