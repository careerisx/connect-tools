package mqtt

import (
	"sync"
	"time"
)

const eventBufferSize = 128

// EventKind identifies one adapter event.
type EventKind string

const (
	// EventInboundMessage reports a received MQTT PUBLISH packet.
	EventInboundMessage EventKind = "inbound_message"
	// EventDisconnected reports an unexpected broker or transport disconnect.
	EventDisconnected EventKind = "disconnected"
	// EventDiagnostic reports an adapter diagnostic, including event overflow.
	EventDiagnostic EventKind = "diagnostic"
)

// Event is an immutable snapshot delivered by an MQTT adapter.
type Event struct {
	Kind       EventKind
	OccurredAt time.Time
	Message    *InboundMessage
	Disconnect *DisconnectInfo
	Diagnostic *EventDiagnosticInfo
}

// InboundMessage is a protocol-neutral received MQTT PUBLISH packet.
type InboundMessage struct {
	Topic                   string
	Payload                 []byte
	QoS                     byte
	Retain                  bool
	Duplicate               bool
	PacketID                uint16
	PacketIDPresent         bool
	Properties              MessageProperties
	SubscriptionIdentifiers []uint32
}

// DisconnectInfo describes an unexpected connection loss.
type DisconnectInfo struct {
	Unexpected      bool
	ReasonCode      *byte
	ReasonString    string
	ServerReference string
	Cause           string
	UserProperties  []UserProperty
}

// EventDiagnosticInfo describes adapter-side event delivery diagnostics.
type EventDiagnosticInfo struct {
	Code    string
	Message string
	Dropped uint64
}

type eventSink struct {
	mu      sync.Mutex
	events  chan Event
	dropped uint64
	closed  bool
}

func newEventSink() *eventSink {
	return &eventSink{events: make(chan Event, eventBufferSize)}
}

func (s *eventSink) channel() <-chan Event {
	return s.events
}

func (s *eventSink) emit(event Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}
	if s.dropped != 0 {
		overflow := Event{
			Kind:       EventDiagnostic,
			OccurredAt: time.Now().UTC(),
			Diagnostic: &EventDiagnosticInfo{
				Code:    "event_overflow",
				Message: "MQTT adapter events were dropped because the consumer was too slow",
				Dropped: s.dropped,
			},
		}
		select {
		case s.events <- overflow:
			s.dropped = 0
		default:
			s.dropped++
			return
		}
	}
	select {
	case s.events <- cloneEvent(event):
	default:
		s.dropped++
	}
}

func (s *eventSink) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	close(s.events)
}

func cloneEvent(event Event) Event {
	clone := event
	if event.Message != nil {
		message := *event.Message
		message.Payload = cloneBytes(event.Message.Payload)
		properties := cloneMessageProperties(&event.Message.Properties)
		message.Properties = *properties
		message.SubscriptionIdentifiers = append([]uint32(nil), event.Message.SubscriptionIdentifiers...)
		clone.Message = &message
	}
	if event.Disconnect != nil {
		disconnect := *event.Disconnect
		disconnect.ReasonCode = cloneByte(event.Disconnect.ReasonCode)
		disconnect.UserProperties = append([]UserProperty(nil), event.Disconnect.UserProperties...)
		clone.Disconnect = &disconnect
	}
	if event.Diagnostic != nil {
		diagnostic := *event.Diagnostic
		clone.Diagnostic = &diagnostic
	}
	return clone
}
