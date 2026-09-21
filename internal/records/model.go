// Package records provides the in-memory debug record model and bounded store.
package records

import (
	"bytes"
	"errors"
	"fmt"
	"time"
)

const (
	// DefaultPerSessionCount is the default number of records retained for one session.
	DefaultPerSessionCount = 10_000
	// DefaultTotalPayloadBytes is the default process-wide retained payload budget.
	DefaultTotalPayloadBytes int64 = 128 << 20
	// DefaultMaxWaiters bounds the fan-out from one appended record. Sixteen
	// waiters keep the normal 16 MiB payload delivery fan-out conservative while
	// still allowing independent UI, CLI, and MCP consumers.
	DefaultMaxWaiters = 16
	// DefaultPageLimit is used when a list request does not specify a limit.
	DefaultPageLimit = 100
	// MaxPageLimit is the largest accepted list page size.
	MaxPageLimit = 500
)

var (
	// ErrInvalidArgument identifies invalid limits, records, filters, or match criteria.
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrResourceExhausted identifies a full waiter pool or exhausted sequence space.
	ErrResourceExhausted = errors.New("resource exhausted")
)

// Direction describes how a record relates to its session.
type Direction uint8

const (
	// DirectionInbound identifies bytes or events received by the local session.
	DirectionInbound Direction = iota + 1
	// DirectionOutbound identifies bytes or events sent by the local session.
	DirectionOutbound
	// DirectionInternal identifies an internal lifecycle or diagnostic event.
	DirectionInternal
)

// Valid reports whether direction is a supported record direction.
func (direction Direction) Valid() bool {
	return direction >= DirectionInbound && direction <= DirectionInternal
}

// Actor identifies who caused a record.
type Actor uint8

const (
	// ActorRemote identifies a remote network peer.
	ActorRemote Actor = iota + 1
	// ActorUser identifies a user action from the Web UI.
	ActorUser
	// ActorCLI identifies a command-line action.
	ActorCLI
	// ActorMCP identifies an MCP tool action.
	ActorMCP
	// ActorSystem identifies an action produced by the runtime itself.
	ActorSystem
)

// Valid reports whether actor is a supported record actor.
func (actor Actor) Valid() bool {
	return actor >= ActorRemote && actor <= ActorSystem
}

// RecordError contains structured error information associated with a record.
type RecordError struct {
	Code      string
	Message   string
	Operation string
	Retryable bool
	Timestamp time.Time
	Details   map[string]string
}

// Record is one immutable debug record as observed by callers.
// Sequence is allocated by Store.Append and caller-provided values are ignored.
type Record struct {
	Sequence         uint64
	Timestamp        time.Time
	SessionID        string
	PeerID           string
	Direction        Direction
	Actor            Actor
	Payload          []byte
	ProtocolMetadata map[string]string
	Error            *RecordError
}

// Limits bounds retained records, payload memory, and concurrent waiters.
// All fields must be greater than zero.
type Limits struct {
	PerSessionCount   int
	TotalPayloadBytes int64
	MaxWaiters        int
}

// DefaultLimits returns the standard bounded-store limits.
func DefaultLimits() Limits {
	return Limits{
		PerSessionCount:   DefaultPerSessionCount,
		TotalPayloadBytes: DefaultTotalPayloadBytes,
		MaxWaiters:        DefaultMaxWaiters,
	}
}

// Filter selects retained records and controls list pagination.
// After is exclusive. A zero Limit uses DefaultPageLimit.
type Filter struct {
	SessionIDs []string
	Direction  *Direction
	Actor      *Actor
	Contains   []byte
	After      uint64
	Limit      int
}

// Match describes the criteria used by Store.Wait.
// After is exclusive.
type Match struct {
	SessionIDs []string
	Direction  *Direction
	Actor      *Actor
	Contains   []byte
	After      uint64
}

// Matches reports whether record satisfies all match criteria.
func (match Match) Matches(record Record) bool {
	return matchesRecord(match.SessionIDs, match.Direction, match.Actor, match.Contains, match.After, record)
}

// Matches reports whether record satisfies all filter criteria. Limit does not
// participate in matching.
func (filter Filter) Matches(record Record) bool {
	return matchesRecord(filter.SessionIDs, filter.Direction, filter.Actor, filter.Contains, filter.After, record)
}

// Page is one ordered page of retained records.
type Page struct {
	Records    []Record
	NextCursor uint64
	HasMore    bool
}

// ClearResult reports resources released by Store.Clear.
type ClearResult struct {
	Records      uint64
	PayloadBytes int64
}

// Metrics reports the exact resources currently retained by a Store.
type Metrics struct {
	Records      uint64
	PayloadBytes int64
	Waiters      int
}

// EventType identifies a retained-record store change.
type EventType uint8

const (
	// EventAppended indicates that a record received its sequence and was stored.
	EventAppended EventType = iota + 1
	// EventCleared indicates that records were removed by an explicit clear.
	EventCleared
	// EventRetentionLimitReached indicates that retention evicted old records.
	EventRetentionLimitReached
)

// Event is a deep-copyable record-store notification.
type Event struct {
	Type                 EventType
	Record               *Record
	SessionIDs           []string
	AffectedRecords      uint64
	AffectedPayloadBytes int64
}

// Observer receives record events after the store has released its lock.
type Observer func(Event)

func validateLimits(limits Limits) error {
	if limits.PerSessionCount <= 0 {
		return fmt.Errorf("%w: per-session record limit must be greater than zero", ErrInvalidArgument)
	}
	if limits.TotalPayloadBytes <= 0 {
		return fmt.Errorf("%w: total payload byte limit must be greater than zero", ErrInvalidArgument)
	}
	if limits.MaxWaiters <= 0 {
		return fmt.Errorf("%w: waiter limit must be greater than zero", ErrInvalidArgument)
	}
	return nil
}

func validateRecord(record Record) error {
	if !record.Direction.Valid() {
		return fmt.Errorf("%w: unsupported direction %d", ErrInvalidArgument, record.Direction)
	}
	if !record.Actor.Valid() {
		return fmt.Errorf("%w: unsupported actor %d", ErrInvalidArgument, record.Actor)
	}
	return nil
}

func validateCriteria(direction *Direction, actor *Actor) error {
	if direction != nil && !direction.Valid() {
		return fmt.Errorf("%w: unsupported direction %d", ErrInvalidArgument, *direction)
	}
	if actor != nil && !actor.Valid() {
		return fmt.Errorf("%w: unsupported actor %d", ErrInvalidArgument, *actor)
	}
	return nil
}

func cloneRecord(record Record) Record {
	cloned := record
	cloned.Payload = bytes.Clone(record.Payload)
	cloned.ProtocolMetadata = cloneStringMap(record.ProtocolMetadata)
	if record.Error != nil {
		clonedError := *record.Error
		clonedError.Details = cloneStringMap(record.Error.Details)
		cloned.Error = &clonedError
	}
	return cloned
}

func cloneEvent(event Event) Event {
	cloned := event
	cloned.SessionIDs = append([]string(nil), event.SessionIDs...)
	if event.Record != nil {
		record := cloneRecord(*event.Record)
		cloned.Record = &record
	}
	return cloned
}

func cloneStringMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	cloned := make(map[string]string, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}

func matchesRecord(sessionIDs []string, direction *Direction, actor *Actor, contains []byte, after uint64, record Record) bool {
	if record.Sequence <= after {
		return false
	}
	if len(sessionIDs) > 0 && !containsString(sessionIDs, record.SessionID) {
		return false
	}
	if direction != nil && record.Direction != *direction {
		return false
	}
	if actor != nil && record.Actor != *actor {
		return false
	}
	return len(contains) == 0 || bytes.Contains(record.Payload, contains)
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
