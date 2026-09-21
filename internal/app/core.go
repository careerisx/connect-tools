package app

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"connect-tools/internal/records"
	"connect-tools/internal/session"
)

// Status describes the currently running Connect Tools core.
type Status struct {
	Version    string
	InstanceID string
	Address    string
	StartedAt  time.Time
	MCPEnabled bool
}

// Core owns transport-neutral application lifecycle state and events.
type Core struct {
	mu        sync.RWMutex
	status    Status
	events    *EventHub
	stopping  atomic.Bool
	mcpChange func(bool) error
}

// NewCore creates an application core with its immutable process metadata.
func NewCore(version, instanceID, address string, startedAt time.Time) *Core {
	return &Core{
		status: Status{
			Version:    version,
			InstanceID: instanceID,
			Address:    address,
			StartedAt:  startedAt,
		},
		events: NewEventHub(),
	}
}

// Status returns a consistent snapshot of the core status.
func (c *Core) Status() Status {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status
}

// SetMCPEnabled updates whether MCP access is enabled.
func (c *Core) SetMCPEnabled(enabled bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mcpChange != nil {
		if err := c.mcpChange(enabled); err != nil {
			return err
		}
	}
	c.status.MCPEnabled = enabled
	return nil
}

// SetMCPChangeHook installs the process-owned persistence callback.
func (c *Core) SetMCPChangeHook(hook func(bool) error) {
	c.mu.Lock()
	c.mcpChange = hook
	c.mu.Unlock()
}

// Shutdown marks the core as stopping, publishes the final transition, and
// closes all event subscriptions exactly once.
func (c *Core) Shutdown() bool {
	if !c.stopping.CompareAndSwap(false, true) {
		return false
	}

	_, _ = c.events.PublishAndClose(Event{
		Type:      EventCoreStopping,
		Timestamp: time.Now(),
		Lifecycle: &CoreLifecycleEvent{Type: EventCoreStopping, Status: c.Status()},
	})
	return true
}

// ObserveProfile publishes a profile event into the global ordered stream.
func (c *Core) ObserveProfile(event ProfileEvent) {
	c.publishBusinessEvent(Event{Type: "profile", Profile: &event})
}

// ObserveSession publishes a session-manager event into the global ordered stream.
func (c *Core) ObserveSession(event session.Event) {
	c.publishBusinessEvent(Event{Type: "session", Session: &event})
}

// ObserveRecord publishes a record-store event into the global ordered stream.
func (c *Core) ObserveRecord(event records.Event) {
	c.publishBusinessEvent(Event{Type: "record", Record: &event})
}

func (c *Core) publishBusinessEvent(event Event) {
	if c == nil || c.stopping.Load() {
		return
	}
	event.Timestamp = time.Now().UTC()
	c.events.Publish(event)
}

// Events subscribes to live core events after the supplied sequence number.
func (c *Core) Events(ctx context.Context, after uint64) (<-chan Event, error) {
	return c.events.Subscribe(ctx, after)
}
