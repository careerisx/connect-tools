package mqtt

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"strconv"
	"sync"
	"time"

	"connect-tools/internal/records"
	"connect-tools/internal/secrets"
	"connect-tools/internal/session"
)

const MaxPayloadBytes = 16 << 20

var (
	ErrRuntimeClosed  = errors.New("MQTT runtime is closed")
	ErrSessionExists  = errors.New("MQTT session already exists")
	ErrSessionMissing = errors.New("MQTT session is not running")
)

type ReconnectPolicy struct {
	Enabled      bool
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
	JitterRatio  float64
	MaxAttempts  uint32
}

type RuntimeConfig struct {
	Adapter              Config
	TLSProfile           TLSProfile
	PasswordRef          string
	InitialSubscriptions []SubscriptionBatch
	Reconnect            ReconnectPolicy
}

type SubscriptionState struct {
	Batch        SubscriptionBatch
	Acknowledged bool
	Ack          SubscribeAck
	LastError    string
}

type endpoint struct {
	id     string
	config RuntimeConfig
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	mu         sync.RWMutex
	client     Client
	connected  bool
	desired    []SubscriptionState
	manualStop bool
	reconnects uint64
	lastError  error
}

type Runtime struct {
	ctx      context.Context
	cancel   context.CancelFunc
	factory  ClientFactory
	sessions *session.Manager
	records  *records.Store
	secrets  secrets.Store

	mu        sync.RWMutex
	endpoints map[string]*endpoint
	closed    bool
	wg        sync.WaitGroup
}

func NewRuntime(parent context.Context, factory ClientFactory, sessions *session.Manager, recordStore *records.Store, secretStore secrets.Store) (*Runtime, error) {
	if parent == nil {
		parent = context.Background()
	}
	if factory == nil || sessions == nil || recordStore == nil || secretStore == nil {
		return nil, errors.New("MQTT runtime dependencies are required")
	}
	ctx, cancel := context.WithCancel(parent)
	return &Runtime{ctx: ctx, cancel: cancel, factory: factory, sessions: sessions, records: recordStore, secrets: secretStore, endpoints: make(map[string]*endpoint)}, nil
}

func (r *Runtime) Start(ctx context.Context, sessionID string, config RuntimeConfig) (session.Session, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if sessionID == "" {
		return session.Session{}, errors.New("session ID is required")
	}
	epCtx, cancel := context.WithCancel(r.ctx)
	ep := &endpoint{id: sessionID, config: cloneRuntimeConfig(config), ctx: epCtx, cancel: cancel, done: make(chan struct{})}
	for _, batch := range config.InitialSubscriptions {
		ep.desired = append(ep.desired, SubscriptionState{Batch: cloneSubscriptionBatch(batch)})
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		cancel()
		return session.Session{}, ErrRuntimeClosed
	}
	if _, exists := r.endpoints[sessionID]; exists {
		r.mu.Unlock()
		cancel()
		return session.Session{}, ErrSessionExists
	}
	r.endpoints[sessionID] = ep
	r.mu.Unlock()

	if _, err := r.sessions.Transition(sessionID, session.StateStarting, nil); err != nil {
		r.removeEndpoint(ep)
		return session.Session{}, err
	}
	client, err := r.connect(ctx, ep)
	if err != nil {
		ep.lastError = err
		r.markError(sessionID, "connect", err, retryableError(err))
		if config.Reconnect.Enabled && retryableError(err) {
			r.wg.Add(1)
			go r.run(ep, nil)
		}
		current, _ := r.sessions.Get(sessionID)
		return current, err
	}
	ep.mu.Lock()
	ep.client = client
	ep.connected = true
	ep.mu.Unlock()
	address := net.JoinHostPort(config.Adapter.Host, strconv.Itoa(config.Adapter.Port))
	_, _ = r.sessions.UpdateAddresses(sessionID, "", address)
	running, err := r.sessions.Transition(sessionID, session.StateRunning, nil)
	if err != nil {
		_ = client.Disconnect(context.Background())
		r.removeEndpoint(ep)
		return session.Session{}, err
	}
	r.restoreSubscriptions(ctx, ep, client)
	r.wg.Add(1)
	go r.run(ep, client)
	return running, nil
}

func (r *Runtime) connect(ctx context.Context, ep *endpoint) (Client, error) {
	config := cloneConfig(ep.config.Adapter)
	if ep.config.PasswordRef != "" {
		password, err := r.secrets.Get(ctx, ep.config.PasswordRef)
		if err != nil {
			return nil, fmt.Errorf("resolve MQTT password: %w", err)
		}
		config.Auth.Password = password
		defer clear(password)
	}
	tlsConfig, err := BuildTLSConfig(ctx, ep.config.TLSProfile, r.secrets)
	if err != nil {
		clear(config.Auth.Password)
		return nil, err
	}
	config.TLS = tlsConfig
	client, err := r.factory.New(config)
	clear(config.Auth.Password)
	if err != nil {
		return nil, err
	}
	if _, err := client.Connect(ctx); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}
	return client, nil
}

func (r *Runtime) run(ep *endpoint, initial Client) {
	defer r.wg.Done()
	defer close(ep.done)
	client := initial
	for {
		if client == nil {
			var err error
			client, err = r.reconnect(ep)
			if err != nil {
				ep.mu.Lock()
				ep.lastError = err
				ep.mu.Unlock()
				if !ep.config.Reconnect.Enabled || !retryableError(err) || ep.ctx.Err() != nil {
					r.removeEndpoint(ep)
					return
				}
				continue
			}
		}
		select {
		case <-ep.ctx.Done():
			_ = client.Disconnect(context.Background())
			r.removeEndpoint(ep)
			return
		case event, ok := <-client.Events():
			if !ok {
				r.markDisconnected(ep, errors.New("MQTT adapter stopped"))
				client = nil
				continue
			}
			switch event.Kind {
			case EventInboundMessage:
				r.recordInbound(ep.id, event)
			case EventDisconnected:
				err := errors.New("MQTT connection lost")
				if event.Disconnect != nil && event.Disconnect.Cause != "" {
					err = errors.New(event.Disconnect.Cause)
				}
				r.markDisconnected(ep, err)
				_ = client.Disconnect(context.Background())
				client = nil
			case EventDiagnostic:
				r.recordDiagnostic(ep.id, event)
			}
		}
	}
}

func (r *Runtime) reconnect(ep *endpoint) (Client, error) {
	policy := normalizedReconnect(ep.config.Reconnect)
	if !policy.Enabled {
		return nil, ErrClientNotConnected
	}
	var attempt uint32
	delay := policy.InitialDelay
	for {
		attempt++
		if policy.MaxAttempts > 0 && attempt > policy.MaxAttempts {
			return nil, errors.New("MQTT reconnect attempts exhausted")
		}
		wait := jitter(delay, policy.JitterRatio)
		timer := time.NewTimer(wait)
		select {
		case <-ep.ctx.Done():
			timer.Stop()
			return nil, ep.ctx.Err()
		case <-timer.C:
		}
		current, err := r.sessions.Get(ep.id)
		if err == nil && current.State == session.StateError {
			_, _ = r.sessions.Transition(ep.id, session.StateStarting, nil)
		}
		client, err := r.connect(ep.ctx, ep)
		if err == nil {
			ep.mu.Lock()
			ep.client = client
			ep.connected = true
			ep.reconnects++
			ep.lastError = nil
			ep.mu.Unlock()
			_, _ = r.sessions.Transition(ep.id, session.StateRunning, nil)
			r.restoreSubscriptions(ep.ctx, ep, client)
			return client, nil
		}
		r.markError(ep.id, "reconnect", err, retryableError(err))
		if !retryableError(err) {
			return nil, err
		}
		delay = time.Duration(float64(delay) * policy.Multiplier)
		if delay > policy.MaxDelay {
			delay = policy.MaxDelay
		}
	}
}

func (r *Runtime) Stop(ctx context.Context, sessionID string) (session.Session, error) {
	ep, err := r.endpoint(sessionID)
	if err != nil {
		current, getErr := r.sessions.Get(sessionID)
		if getErr == nil && current.State == session.StateError {
			return r.sessions.Transition(sessionID, session.StateStopped, nil)
		}
		return session.Session{}, err
	}
	ep.mu.Lock()
	ep.manualStop = true
	client := ep.client
	ep.mu.Unlock()
	current, err := r.sessions.Get(sessionID)
	if err == nil {
		if current.State == session.StateRunning || current.State == session.StateStarting {
			_, _ = r.sessions.Transition(sessionID, session.StateStopping, nil)
		}
	}
	ep.cancel()
	if client != nil {
		_ = client.Disconnect(ctx)
	}
	select {
	case <-ep.done:
	case <-ctx.Done():
		return session.Session{}, ctx.Err()
	}
	current, err = r.sessions.Get(sessionID)
	if err != nil {
		return session.Session{}, err
	}
	if current.State == session.StateStopping || current.State == session.StateError {
		return r.sessions.Transition(sessionID, session.StateStopped, nil)
	}
	return current, nil
}

func (r *Runtime) Publish(ctx context.Context, sessionID string, message Publish, actor records.Actor) (PublishAck, error) {
	if len(message.Payload) > MaxPayloadBytes {
		return PublishAck{}, fmt.Errorf("payload exceeds %d bytes", MaxPayloadBytes)
	}
	ep, err := r.endpoint(sessionID)
	if err != nil {
		return PublishAck{}, err
	}
	client, err := connectedClient(ep)
	if err != nil {
		return PublishAck{}, err
	}
	ack, err := client.Publish(ctx, message)
	if err != nil {
		return ack, err
	}
	metadata := map[string]string{"protocol": "mqtt", "topic": message.Topic, "qos": strconv.Itoa(int(message.QoS)), "retain": strconv.FormatBool(message.Retain)}
	_, _ = r.records.Append(records.Record{SessionID: sessionID, Direction: records.DirectionOutbound, Actor: actor, Payload: append([]byte(nil), message.Payload...), ProtocolMetadata: metadata})
	return ack, nil
}

func (r *Runtime) Subscribe(ctx context.Context, sessionID string, batch SubscriptionBatch) (SubscribeAck, error) {
	ep, err := r.endpoint(sessionID)
	if err != nil {
		return SubscribeAck{}, err
	}
	client, err := connectedClient(ep)
	if err != nil {
		return SubscribeAck{}, err
	}
	ack, err := client.Subscribe(ctx, batch)
	state := SubscriptionState{Batch: cloneSubscriptionBatch(batch), Ack: ack, Acknowledged: err == nil}
	if err != nil {
		state.LastError = err.Error()
	}
	ep.mu.Lock()
	ep.desired = upsertSubscriptionState(ep.desired, state)
	ep.mu.Unlock()
	return ack, err
}

func (r *Runtime) Unsubscribe(ctx context.Context, sessionID string, filters []string) (UnsubscribeAck, error) {
	ep, err := r.endpoint(sessionID)
	if err != nil {
		return UnsubscribeAck{}, err
	}
	client, err := connectedClient(ep)
	if err != nil {
		return UnsubscribeAck{}, err
	}
	ack, err := client.Unsubscribe(ctx, filters)
	if err == nil {
		ep.mu.Lock()
		ep.desired = removeSubscriptionFilters(ep.desired, filters)
		ep.mu.Unlock()
	}
	return ack, err
}

func (r *Runtime) ListSubscriptions(sessionID string) ([]SubscriptionState, error) {
	ep, err := r.endpoint(sessionID)
	if err != nil {
		return nil, err
	}
	ep.mu.RLock()
	defer ep.mu.RUnlock()
	result := make([]SubscriptionState, len(ep.desired))
	for index, value := range ep.desired {
		result[index] = value
		result[index].Batch = cloneSubscriptionBatch(value.Batch)
	}
	return result, nil
}

func (r *Runtime) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	endpoints := make([]*endpoint, 0, len(r.endpoints))
	for _, ep := range r.endpoints {
		endpoints = append(endpoints, ep)
	}
	r.mu.Unlock()
	r.cancel()
	for _, ep := range endpoints {
		ep.cancel()
	}
	r.wg.Wait()
	return nil
}

func (r *Runtime) endpoint(id string) (*endpoint, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ep := r.endpoints[id]
	if ep == nil {
		return nil, fmt.Errorf("%w: %s", ErrSessionMissing, id)
	}
	return ep, nil
}

func (r *Runtime) removeEndpoint(ep *endpoint) {
	r.mu.Lock()
	if r.endpoints[ep.id] == ep {
		delete(r.endpoints, ep.id)
	}
	r.mu.Unlock()
}

func (r *Runtime) markDisconnected(ep *endpoint, err error) {
	ep.mu.Lock()
	ep.connected = false
	ep.client = nil
	ep.lastError = err
	ep.mu.Unlock()
	r.markError(ep.id, "connection", err, true)
}

func (r *Runtime) markError(id, operation string, err error, retryable bool) {
	_, _ = r.sessions.Transition(id, session.StateError, &session.Error{Code: "mqtt_error", Message: safeError(err), Operation: operation, Retryable: retryable, Timestamp: time.Now().UTC()})
	_, _ = r.records.Append(records.Record{SessionID: id, Direction: records.DirectionInternal, Actor: records.ActorSystem, Error: &records.RecordError{Code: "mqtt_error", Message: safeError(err), Operation: operation, Retryable: retryable, Timestamp: time.Now().UTC()}})
}

func (r *Runtime) recordInbound(id string, event Event) {
	if event.Message == nil {
		return
	}
	message := event.Message
	metadata := map[string]string{"protocol": "mqtt", "topic": message.Topic, "qos": strconv.Itoa(int(message.QoS)), "retain": strconv.FormatBool(message.Retain), "duplicate": strconv.FormatBool(message.Duplicate)}
	_, _ = r.records.Append(records.Record{SessionID: id, Direction: records.DirectionInbound, Actor: records.ActorRemote, Payload: append([]byte(nil), message.Payload...), ProtocolMetadata: metadata})
}

func (r *Runtime) recordDiagnostic(id string, event Event) {
	if event.Diagnostic == nil {
		return
	}
	_, _ = r.records.Append(records.Record{SessionID: id, Direction: records.DirectionInternal, Actor: records.ActorSystem, ProtocolMetadata: map[string]string{"protocol": "mqtt", "dropped_events": strconv.FormatUint(event.Diagnostic.Dropped, 10)}, Error: &records.RecordError{Code: event.Diagnostic.Code, Message: event.Diagnostic.Message, Operation: "event", Retryable: true, Timestamp: event.OccurredAt}})
}

func (r *Runtime) restoreSubscriptions(ctx context.Context, ep *endpoint, client Client) {
	ep.mu.RLock()
	states := append([]SubscriptionState(nil), ep.desired...)
	ep.mu.RUnlock()
	for _, state := range states {
		ack, err := client.Subscribe(ctx, state.Batch)
		state.Ack = ack
		state.Acknowledged = err == nil
		if err != nil {
			state.LastError = safeError(err)
		}
		ep.mu.Lock()
		ep.desired = upsertSubscriptionState(ep.desired, state)
		ep.mu.Unlock()
	}
}

func connectedClient(ep *endpoint) (Client, error) {
	ep.mu.RLock()
	defer ep.mu.RUnlock()
	if !ep.connected || ep.client == nil {
		return nil, ErrClientNotConnected
	}
	return ep.client, nil
}

func normalizedReconnect(policy ReconnectPolicy) ReconnectPolicy {
	if policy.InitialDelay <= 0 {
		policy.InitialDelay = time.Second
	}
	if policy.MaxDelay <= 0 {
		policy.MaxDelay = 30 * time.Second
	}
	if policy.Multiplier < 1 {
		policy.Multiplier = 2
	}
	if policy.JitterRatio < 0 {
		policy.JitterRatio = 0
	}
	if policy.JitterRatio > 1 {
		policy.JitterRatio = 1
	}
	return policy
}

func jitter(delay time.Duration, ratio float64) time.Duration {
	if ratio == 0 {
		return delay
	}
	factor := 1 - ratio + rand.Float64()*(2*ratio)
	return time.Duration(float64(delay) * factor)
}

func retryableError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, ErrInvalidConfig) || errors.Is(err, ErrInvalidOperation) || errors.Is(err, ErrUnsupportedVersion) {
		return false
	}
	var diagnostic *DiagnosticError
	if errors.As(err, &diagnostic) {
		return diagnostic.Retryable
	}
	return true
}

func safeError(err error) string {
	if err == nil {
		return ""
	}
	var diagnostic *DiagnosticError
	if errors.As(err, &diagnostic) {
		return diagnostic.Message
	}
	return err.Error()
}

func cloneRuntimeConfig(config RuntimeConfig) RuntimeConfig {
	clone := config
	clone.Adapter = cloneConfig(config.Adapter)
	clone.InitialSubscriptions = make([]SubscriptionBatch, len(config.InitialSubscriptions))
	for i := range config.InitialSubscriptions {
		clone.InitialSubscriptions[i] = cloneSubscriptionBatch(config.InitialSubscriptions[i])
	}
	return clone
}

func upsertSubscriptionState(states []SubscriptionState, state SubscriptionState) []SubscriptionState {
	filters := make(map[string]struct{}, len(state.Batch.Subscriptions))
	for _, item := range state.Batch.Subscriptions {
		filters[item.Filter] = struct{}{}
	}
	states = removeSubscriptionFilters(states, mapKeys(filters))
	return append(states, state)
}

func removeSubscriptionFilters(states []SubscriptionState, filters []string) []SubscriptionState {
	remove := make(map[string]struct{}, len(filters))
	for _, filter := range filters {
		remove[filter] = struct{}{}
	}
	result := make([]SubscriptionState, 0, len(states))
	for _, state := range states {
		batch := cloneSubscriptionBatch(state.Batch)
		batch.Subscriptions = batch.Subscriptions[:0]
		for _, item := range state.Batch.Subscriptions {
			if _, exists := remove[item.Filter]; !exists {
				batch.Subscriptions = append(batch.Subscriptions, item)
			}
		}
		if len(batch.Subscriptions) > 0 {
			state.Batch = batch
			result = append(result, state)
		}
	}
	return result
}

func mapKeys(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	return result
}
