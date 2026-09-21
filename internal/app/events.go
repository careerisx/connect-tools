package app

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	"connect-tools/internal/records"
	"connect-tools/internal/session"
)

const (
	// EventCoreStopping is published when the core accepts a shutdown request.
	EventCoreStopping = "core.stopping"

	maxEventSubscribers   = 16
	eventSubscriberBuffer = 64
	maxEventPayloadBytes  = 64 << 10
)

// ErrTooManySubscribers indicates that the event subscriber limit was reached.
var ErrTooManySubscribers = errors.New("too many event subscribers")

// Event describes a state change emitted by the application core.
type Event struct {
	Sequence  uint64
	Timestamp time.Time
	Type      string
	Payload   []byte
	// PayloadTruncated reports that Payload was shortened to the event size limit.
	PayloadTruncated bool
	Profile          *ProfileEvent
	Session          *session.Event
	Record           *records.Event
	Lifecycle        *CoreLifecycleEvent
}

// ProfileEventType identifies a saved-profile change.
type ProfileEventType uint8

const (
	// ProfileEventCreated indicates that a profile was created.
	ProfileEventCreated ProfileEventType = iota + 1
	// ProfileEventUpdated indicates that a profile was fully replaced.
	ProfileEventUpdated
	// ProfileEventDeleted indicates that a profile was deleted.
	ProfileEventDeleted
)

// ProfileEvent deliberately carries only identity. Callers reload the profile
// so large Will payloads and credential references never enter the live stream.
type ProfileEvent struct {
	Type      ProfileEventType
	ProfileID string
}

// CoreLifecycleEvent describes a core process lifecycle transition.
type CoreLifecycleEvent struct {
	Type   string
	Status Status
}

// EventHub assigns ordered sequence numbers and fans events out to subscribers.
// Its zero value is ready to use.
type EventHub struct {
	mu               sync.Mutex
	sequence         uint64
	nextSubscriberID uint64
	subscribers      map[uint64]*eventSubscriber
	closed           bool
}

type eventSubscriber struct {
	events  chan Event
	removed chan struct{}
	after   uint64
}

// NewEventHub creates an empty event hub.
func NewEventHub() *EventHub {
	return &EventHub{
		subscribers: make(map[uint64]*eventSubscriber),
	}
}

// Publish assigns the next sequence number and delivers the event without
// waiting for subscribers. A subscriber whose buffer is full is closed and
// removed so it can reconnect using its last received sequence.
func (h *EventHub) Publish(event Event) uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return h.sequence
	}

	return h.publishLocked(event)
}

// PublishAndClose publishes one final event and then closes the hub. The
// operation is atomic with respect to publishers and subscribers, so the
// supplied event is the final event observed by every current subscriber.
// Repeated calls leave the hub closed and do not advance its sequence.
func (h *EventHub) PublishAndClose(event Event) (uint64, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return h.sequence, false
	}

	sequence := h.publishLocked(event)
	h.closeLocked()
	return sequence, true
}

// Close idempotently closes the hub and all current subscriptions. New
// subscriptions receive an already-closed channel.
func (h *EventHub) Close() bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return false
	}

	h.closeLocked()
	return true
}

func (h *EventHub) publishLocked(event Event) uint64 {
	h.sequence++
	event.Sequence = h.sequence
	event = normalizeEvent(event)
	if len(event.Payload) > maxEventPayloadBytes {
		event.Payload = bytes.Clone(event.Payload[:maxEventPayloadBytes])
		event.PayloadTruncated = true
	}

	for id, subscriber := range h.subscribers {
		if event.Sequence <= subscriber.after {
			continue
		}
		if len(subscriber.events) == cap(subscriber.events) {
			delete(h.subscribers, id)
			closeSubscriber(subscriber)
			continue
		}

		subscriber.events <- cloneEvent(event)
	}

	return event.Sequence
}

func (h *EventHub) closeLocked() {
	h.closed = true
	for id, subscriber := range h.subscribers {
		delete(h.subscribers, id)
		closeSubscriber(subscriber)
	}
}

// Subscribe returns live events after the supplied sequence. Canceling ctx
// closes and removes the subscription safely.
func (h *EventHub) Subscribe(ctx context.Context, after uint64) (<-chan Event, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	h.mu.Lock()
	if h.closed {
		events := make(chan Event)
		close(events)
		h.mu.Unlock()
		return events, nil
	}
	if len(h.subscribers) >= maxEventSubscribers {
		h.mu.Unlock()
		return nil, ErrTooManySubscribers
	}

	subscriber := &eventSubscriber{
		events:  make(chan Event, eventSubscriberBuffer),
		removed: make(chan struct{}),
		after:   after,
	}

	select {
	case <-ctx.Done():
		close(subscriber.events)
		close(subscriber.removed)
		h.mu.Unlock()
		return subscriber.events, nil
	default:
	}

	if h.subscribers == nil {
		h.subscribers = make(map[uint64]*eventSubscriber)
	}
	h.nextSubscriberID++
	id := h.nextSubscriberID
	h.subscribers[id] = subscriber
	h.mu.Unlock()

	if ctx.Done() != nil {
		go h.removeSubscriberOnCancel(ctx, id, subscriber)
	}

	return subscriber.events, nil
}

func (h *EventHub) removeSubscriberOnCancel(ctx context.Context, id uint64, subscriber *eventSubscriber) {
	select {
	case <-ctx.Done():
	case <-subscriber.removed:
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if current, ok := h.subscribers[id]; ok && current == subscriber {
		delete(h.subscribers, id)
		closeSubscriber(subscriber)
	}
}

func closeSubscriber(subscriber *eventSubscriber) {
	close(subscriber.events)
	close(subscriber.removed)
}

func cloneEvent(event Event) Event {
	event.Payload = bytes.Clone(event.Payload)
	if event.Profile != nil {
		profileEvent := *event.Profile
		event.Profile = &profileEvent
	}
	if event.Session != nil {
		sessionEvent := *event.Session
		sessionEvent.Session = event.Session.Session.Clone()
		event.Session = &sessionEvent
	}
	if event.Record != nil {
		recordEvent := *event.Record
		recordEvent.SessionIDs = append([]string(nil), event.Record.SessionIDs...)
		if event.Record.Record != nil {
			record := cloneRecordForEvent(*event.Record.Record)
			recordEvent.Record = &record
		}
		event.Record = &recordEvent
	}
	if event.Lifecycle != nil {
		lifecycle := *event.Lifecycle
		event.Lifecycle = &lifecycle
	}
	return event
}

func normalizeEvent(event Event) Event {
	event = cloneEvent(event)
	if event.Record != nil && event.Record.Record != nil && len(event.Record.Record.Payload) > maxEventPayloadBytes {
		event.Record.Record.Payload = bytes.Clone(event.Record.Record.Payload[:maxEventPayloadBytes])
		event.PayloadTruncated = true
	}
	return event
}

func cloneRecordForEvent(record records.Record) records.Record {
	clone := record
	clone.Payload = bytes.Clone(record.Payload)
	if record.ProtocolMetadata != nil {
		clone.ProtocolMetadata = make(map[string]string, len(record.ProtocolMetadata))
		for key, value := range record.ProtocolMetadata {
			clone.ProtocolMetadata[key] = value
		}
	}
	if record.Error != nil {
		recordError := *record.Error
		if record.Error.Details != nil {
			recordError.Details = make(map[string]string, len(record.Error.Details))
			for key, value := range record.Error.Details {
				recordError.Details[key] = value
			}
		}
		clone.Error = &recordError
	}
	return clone
}
