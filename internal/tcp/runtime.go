package tcp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"sync"
	"syscall"
	"time"

	"connect-tools/internal/framing"
	"connect-tools/internal/records"
	"connect-tools/internal/session"
)

// Runtime owns all active TCP listeners, connections, queues, and goroutines.
type Runtime struct {
	ctx      context.Context
	cancel   context.CancelFunc
	sessions *session.Manager
	records  *records.Store

	mu        sync.Mutex
	closed    bool
	clients   map[string]*clientEndpoint
	servers   map[string]*serverEndpoint
	peers     map[string]*peerEndpoint
	starts    map[string]*pendingStart
	wg        sync.WaitGroup
	startWG   sync.WaitGroup
	closeDone chan struct{}
}

type pendingStart struct {
	ctx       context.Context
	request   context.Context
	cancel    context.CancelFunc
	stopHooks []func() bool
	done      chan struct{}
	canceled  bool // guarded by Runtime.mu
	committed bool // guarded by Runtime.mu
}

// NewRuntime creates a TCP runtime bound to parent. Canceling parent closes
// long-lived endpoints independently of individual RPC or CLI request contexts.
func NewRuntime(parent context.Context, sessions *session.Manager, recordStore *records.Store) (*Runtime, error) {
	if sessions == nil || recordStore == nil {
		return nil, fmt.Errorf("%w: session manager and record store are required", ErrInvalidConfig)
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	runtime := &Runtime{
		ctx:       ctx,
		cancel:    cancel,
		sessions:  sessions,
		records:   recordStore,
		clients:   make(map[string]*clientEndpoint),
		servers:   make(map[string]*serverEndpoint),
		peers:     make(map[string]*peerEndpoint),
		starts:    make(map[string]*pendingStart),
		closeDone: make(chan struct{}),
	}
	go func() {
		<-ctx.Done()
		_ = runtime.Close()
	}()
	return runtime, nil
}

// Send queues payload without blocking on socket I/O. A server target
// broadcasts to a deterministic snapshot of its current peers.
func (runtime *Runtime) Send(ctx context.Context, targetSessionID string, payload []byte, actor records.Actor) (SendResult, error) {
	if err := callContextError(ctx); err != nil {
		return SendResult{}, err
	}
	runtime.mu.Lock()
	closed := runtime.closed
	runtime.mu.Unlock()
	if closed {
		return SendResult{}, ErrClosed
	}
	if len(payload) > MaxPayloadBytes {
		return SendResult{}, ErrPayloadTooLarge
	}
	if !actor.Valid() || actor == records.ActorRemote {
		return SendResult{}, fmt.Errorf("%w: invalid outbound actor", ErrInvalidConfig)
	}
	snapshot, err := runtime.sessions.Get(targetSessionID)
	if err != nil {
		return SendResult{}, err
	}
	if snapshot.State != session.StateRunning {
		return SendResult{}, ErrEndpointNotRunning
	}

	runtime.mu.Lock()
	if runtime.closed {
		runtime.mu.Unlock()
		return SendResult{}, ErrClosed
	}
	switch snapshot.Kind {
	case session.KindTCPClient:
		endpoint := runtime.clients[targetSessionID]
		runtime.mu.Unlock()
		if endpoint == nil {
			return SendResult{}, ErrEndpointNotFound
		}
		if err := endpoint.queue.Enqueue(payload, actor); err != nil {
			return SendResult{Accepted: false}, err
		}
		return SendResult{Accepted: true}, nil
	case session.KindTCPPeer:
		endpoint := runtime.peers[targetSessionID]
		runtime.mu.Unlock()
		if endpoint == nil {
			return SendResult{}, ErrEndpointNotFound
		}
		if err := endpoint.queue.Enqueue(payload, actor); err != nil {
			return SendResult{Accepted: false}, err
		}
		return SendResult{Accepted: true}, nil
	case session.KindTCPServer:
		server := runtime.servers[targetSessionID]
		if server == nil {
			runtime.mu.Unlock()
			return SendResult{}, ErrEndpointNotFound
		}
		peers := make([]*peerEndpoint, 0, len(server.peers))
		for _, peer := range server.peers {
			peers = append(peers, peer)
		}
		runtime.mu.Unlock()
		sort.Slice(peers, func(left, right int) bool { return peers[left].id < peers[right].id })
		result := SendResult{Accepted: len(peers) > 0, PeerResults: make([]PeerSendResult, 0, len(peers))}
		if len(peers) == 0 {
			return result, nil
		}
		for _, peer := range peers {
			peerResult := PeerSendResult{PeerSessionID: peer.id}
			peerResult.Error = peer.queue.Enqueue(payload, actor)
			peerResult.Accepted = peerResult.Error == nil
			if peerResult.Error != nil {
				result.Accepted = false
			}
			result.PeerResults = append(result.PeerResults, peerResult)
		}
		return result, nil
	default:
		runtime.mu.Unlock()
		return SendResult{}, ErrEndpointNotFound
	}
}

// Stop closes one TCP client, server, or peer and waits for its owned
// goroutines to finish. It is safe to call repeatedly.
func (runtime *Runtime) Stop(ctx context.Context, sessionID string) (session.Session, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	runtime.mu.Lock()
	client := runtime.clients[sessionID]
	server := runtime.servers[sessionID]
	peer := runtime.peers[sessionID]
	starting := runtime.starts[sessionID]
	if starting != nil && !starting.committed {
		starting.canceled = true
		starting.cancel()
	}
	runtime.mu.Unlock()
	if starting != nil {
		runtime.beginStopping(sessionID)
		select {
		case <-starting.done:
		case <-ctx.Done():
			return session.Session{}, ctx.Err()
		}
		runtime.mu.Lock()
		registered := runtime.clients[sessionID] != nil || runtime.servers[sessionID] != nil || runtime.peers[sessionID] != nil
		runtime.mu.Unlock()
		if registered {
			return runtime.Stop(ctx, sessionID)
		}
		runtime.finishStopped(sessionID)
		return runtime.sessions.Get(sessionID)
	}

	if client != nil {
		return runtime.stopClient(ctx, client)
	}
	if server != nil {
		return runtime.stopServer(ctx, server)
	}
	if peer != nil {
		return runtime.stopPeer(ctx, peer)
	}
	snapshot, err := runtime.sessions.Get(sessionID)
	if err != nil {
		return session.Session{}, err
	}
	if snapshot.State == session.StateStopped {
		return snapshot, nil
	}
	if snapshot.State == session.StateError || snapshot.State == session.StateStopping {
		runtime.finishStopped(sessionID)
		return runtime.sessions.Get(sessionID)
	}
	return session.Session{}, ErrEndpointNotFound
}

// ListPeers returns active child sessions for serverSessionID in deterministic
// creation-time and ID order.
func (runtime *Runtime) ListPeers(ctx context.Context, serverSessionID string) ([]session.Session, error) {
	if err := callContextError(ctx); err != nil {
		return nil, err
	}
	runtime.mu.Lock()
	if runtime.closed {
		runtime.mu.Unlock()
		return nil, ErrClosed
	}
	server := runtime.servers[serverSessionID]
	if server == nil {
		runtime.mu.Unlock()
		return nil, ErrEndpointNotFound
	}
	ids := make([]string, 0, len(server.peers))
	for id := range server.peers {
		ids = append(ids, id)
	}
	runtime.mu.Unlock()

	result := make([]session.Session, 0, len(ids))
	for _, id := range ids {
		value, err := runtime.sessions.Get(id)
		if err == nil && value.State == session.StateRunning {
			result = append(result, value)
		}
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].CreatedAt.Equal(result[right].CreatedAt) {
			return result[left].ID < result[right].ID
		}
		return result[left].CreatedAt.Before(result[right].CreatedAt)
	})
	return result, nil
}

// Close idempotently rejects new operations, closes every listener and
// connection, cancels reconnect delays, and waits for all runtime goroutines.
func (runtime *Runtime) Close() error {
	runtime.mu.Lock()
	if runtime.closed {
		done := runtime.closeDone
		runtime.mu.Unlock()
		<-done
		return nil
	}
	runtime.closed = true
	runtime.cancel()
	clients := make([]*clientEndpoint, 0, len(runtime.clients))
	for _, client := range runtime.clients {
		clients = append(clients, client)
	}
	servers := make([]*serverEndpoint, 0, len(runtime.servers))
	for _, server := range runtime.servers {
		servers = append(servers, server)
	}
	peers := make([]*peerEndpoint, 0, len(runtime.peers))
	for _, peer := range runtime.peers {
		peers = append(peers, peer)
	}
	starts := make([]*pendingStart, 0, len(runtime.starts))
	startIDs := make([]string, 0, len(runtime.starts))
	for id, starting := range runtime.starts {
		starts = append(starts, starting)
		startIDs = append(startIDs, id)
	}
	for _, starting := range starts {
		if !starting.committed {
			starting.canceled = true
		}
		starting.cancel()
	}
	for _, client := range clients {
		client.cancel()
		client.queue.Close()
		client.closeConnection()
	}
	for _, server := range servers {
		server.cancel()
		_ = server.listener.Close()
	}
	for _, peer := range peers {
		peer.cancel()
		peer.queue.Close()
		_ = peer.conn.Close()
	}
	runtime.mu.Unlock()

	runtime.startWG.Wait()
	runtime.wg.Wait()
	for _, id := range startIDs {
		runtime.finishStopped(id)
	}
	for _, client := range clients {
		<-client.cleanupDone
	}
	for _, server := range servers {
		<-server.cleanupDone
	}
	for _, peer := range peers {
		runtime.finishStopped(peer.id)
		_, _ = runtime.sessions.Remove(peer.id)
	}
	runtime.mu.Lock()
	runtime.clients = make(map[string]*clientEndpoint)
	runtime.servers = make(map[string]*serverEndpoint)
	runtime.peers = make(map[string]*peerEndpoint)
	runtime.starts = make(map[string]*pendingStart)
	runtime.mu.Unlock()
	close(runtime.closeDone)
	return nil
}

func (runtime *Runtime) runConnection(ctx context.Context, conn *net.TCPConn, decoder framing.Decoder, queue *sendQueue, sessionID, peerID string, readTimeout, writeTimeout time.Duration) error {
	connectionContext, cancelConnection := context.WithCancel(ctx)
	defer cancelConnection()
	failures := make(chan error, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		failures <- runtime.readLoop(connectionContext, conn, decoder, sessionID, peerID, readTimeout)
	}()
	go func() {
		defer workers.Done()
		failures <- runtime.writeLoop(connectionContext, conn, queue, sessionID, peerID, writeTimeout)
	}()

	err := <-failures
	cancelConnection()
	_ = conn.Close()
	workers.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func (runtime *Runtime) readLoop(ctx context.Context, conn *net.TCPConn, decoder framing.Decoder, sessionID, peerID string, timeout time.Duration) error {
	buffer := make([]byte, DefaultReadBufferBytes)
	for {
		if err := setReadDeadline(conn, timeout); err != nil {
			return &networkOperationError{operation: "tcp.read_deadline", err: err}
		}
		count, err := conn.Read(buffer)
		if count > 0 {
			frames, decodeErr := decoder.Push(buffer[:count])
			for _, frame := range frames {
				_, _ = runtime.records.Append(records.Record{SessionID: sessionID, PeerID: peerID, Direction: records.DirectionInbound, Actor: records.ActorRemote, Payload: frame})
			}
			if decodeErr != nil {
				return &networkOperationError{operation: "tcp.decode", err: decodeErr}
			}
		}
		if err != nil {
			return &networkOperationError{operation: "tcp.read", err: err}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}

func (runtime *Runtime) writeLoop(ctx context.Context, conn *net.TCPConn, queue *sendQueue, sessionID, peerID string, timeout time.Duration) error {
	for {
		item, err := queue.Dequeue(ctx)
		if err != nil {
			return err
		}
		if err := setWriteDeadline(conn, timeout); err != nil {
			return &networkOperationError{operation: "tcp.write_deadline", err: err}
		}
		written, err := writeAll(conn, item.payload)
		if err != nil {
			return &networkOperationError{operation: "tcp.write", err: err}
		}
		if written != len(item.payload) {
			err = io.ErrShortWrite
			return &networkOperationError{operation: "tcp.write", err: err}
		}
		_, _ = runtime.records.Append(records.Record{SessionID: sessionID, PeerID: peerID, Direction: records.DirectionOutbound, Actor: item.actor, Payload: item.payload})
	}
}

type networkOperationError struct {
	operation string
	err       error
}

func (failure *networkOperationError) Error() string { return failure.err.Error() }
func (failure *networkOperationError) Unwrap() error { return failure.err }

func failureOperation(err error, fallback string) string {
	var failure *networkOperationError
	if errors.As(err, &failure) {
		return failure.operation
	}
	return fallback
}

func isRetryableNetworkFailure(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, net.ErrClosed) {
		return false
	}
	if errors.Is(err, framing.ErrFrameTooLarge) || errors.Is(err, framing.ErrInvalidConfig) || errors.Is(err, ErrInvalidConfig) || errors.Is(err, ErrEndpointNotFound) || errors.Is(err, ErrEndpointNotRunning) {
		return false
	}
	operation := failureOperation(err, "")
	switch operation {
	case "tcp.decode", "tcp.configure", "tcp.read_deadline", "tcp.write_deadline":
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.ErrClosedPipe) {
		return true
	}
	var networkError net.Error
	if errors.As(err, &networkError) {
		if networkError.Timeout() || networkError.Temporary() {
			return true
		}
	}
	var dnsError *net.DNSError
	if errors.As(err, &dnsError) {
		return false
	}
	var addressError *net.AddrError
	if errors.As(err, &addressError) {
		return false
	}
	var operationError *net.OpError
	if !errors.As(err, &operationError) {
		return false
	}
	switch operationError.Op {
	case "accept", "dial", "read", "write":
	default:
		return false
	}
	var syscallError *os.SyscallError
	if errors.As(err, &syscallError) {
		return true
	}
	var errno syscall.Errno
	return errors.As(err, &errno)
}

func writeAll(conn net.Conn, payload []byte) (int, error) {
	written := 0
	for written < len(payload) {
		count, err := conn.Write(payload[written:])
		written += count
		if err != nil {
			return written, err
		}
		if count == 0 {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

func setReadDeadline(conn net.Conn, timeout time.Duration) error {
	if timeout == 0 {
		return conn.SetReadDeadline(time.Time{})
	}
	return conn.SetReadDeadline(time.Now().Add(timeout))
}

func setWriteDeadline(conn net.Conn, timeout time.Duration) error {
	if timeout == 0 {
		return conn.SetWriteDeadline(time.Time{})
	}
	return conn.SetWriteDeadline(time.Now().Add(timeout))
}

func configureTCPConn(conn *net.TCPConn, keepAlive bool, keepAlivePeriod time.Duration, noDelay bool) error {
	if err := conn.SetKeepAlive(keepAlive); err != nil {
		return fmt.Errorf("set TCP keepalive: %w", err)
	}
	if keepAlive && keepAlivePeriod > 0 {
		if err := conn.SetKeepAlivePeriod(keepAlivePeriod); err != nil {
			return fmt.Errorf("set TCP keepalive period: %w", err)
		}
	}
	if err := conn.SetNoDelay(noDelay); err != nil {
		return fmt.Errorf("set TCP no-delay: %w", err)
	}
	return nil
}

func (runtime *Runtime) appendNetworkError(sessionID, peerID, operation string, err error, retryable bool) {
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	_, _ = runtime.records.Append(records.Record{
		SessionID: sessionID,
		PeerID:    peerID,
		Direction: records.DirectionInternal,
		Actor:     records.ActorSystem,
		Error:     &records.RecordError{Code: "network_error", Message: err.Error(), Operation: operation, Retryable: retryable, Timestamp: time.Now().UTC()},
	})
}

func sessionFailure(operation string, err error, retryable bool) *session.Error {
	code := "network_error"
	if errors.Is(err, context.Canceled) || (errors.Is(err, context.DeadlineExceeded) && !retryable) {
		code = "canceled"
	} else if errors.Is(err, context.DeadlineExceeded) {
		code = "timeout"
	}
	return &session.Error{Code: code, Message: err.Error(), Operation: operation, Retryable: retryable, Timestamp: time.Now().UTC()}
}

func (runtime *Runtime) markError(id, operation string, err error, retryable bool) {
	current, getErr := runtime.sessions.Get(id)
	if getErr != nil || current.State == session.StateError || current.State == session.StateStopped || current.State == session.StateStopping {
		return
	}
	_, _ = runtime.sessions.Transition(id, session.StateError, sessionFailure(operation, err, retryable))
}

func (runtime *Runtime) settleStartFailure(id, operation string, err error, retryable, canceled bool) {
	if canceled || errors.Is(err, ErrClosed) || errors.Is(err, ErrEndpointNotRunning) {
		runtime.finishStopped(id)
		return
	}
	runtime.markError(id, operation, err, retryable)
}

func (runtime *Runtime) startWasCanceled(id string, starting *pendingStart, err error) bool {
	if errors.Is(err, ErrClosed) || errors.Is(err, context.Canceled) {
		return true
	}
	if starting.request != nil && starting.request.Err() != nil {
		return true
	}
	runtime.mu.Lock()
	closed := runtime.closed
	runtime.mu.Unlock()
	if closed {
		return true
	}
	current, getErr := runtime.sessions.Get(id)
	return getErr == nil && current.State == session.StateStopping
}

func (runtime *Runtime) beginStopping(id string) {
	current, err := runtime.sessions.Get(id)
	if err != nil {
		return
	}
	if current.State == session.StateStarting || current.State == session.StateRunning {
		_, _ = runtime.sessions.Transition(id, session.StateStopping, nil)
	}
}

func (runtime *Runtime) finishStopped(id string) {
	current, err := runtime.sessions.Get(id)
	if err != nil || current.State == session.StateStopped {
		return
	}
	if current.State == session.StateStarting || current.State == session.StateRunning {
		_, _ = runtime.sessions.Transition(id, session.StateStopping, nil)
		current, err = runtime.sessions.Get(id)
		if err != nil {
			return
		}
	}
	if current.State == session.StateStopping || current.State == session.StateError {
		_, _ = runtime.sessions.Transition(id, session.StateStopped, nil)
	}
}

func callContextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

func (runtime *Runtime) beginStart(sessionID string, requestContext context.Context, timeout time.Duration) (context.Context, *pendingStart, error) {
	ctx, cancel := context.WithCancel(runtime.ctx)
	stopHooks := make([]func() bool, 0, 2)
	if timeout > 0 {
		var timeoutCancel context.CancelFunc
		ctx, timeoutCancel = context.WithTimeout(ctx, timeout)
		previousCancel := cancel
		cancel = func() {
			timeoutCancel()
			previousCancel()
		}
	}
	starting := &pendingStart{ctx: ctx, request: requestContext, cancel: cancel, stopHooks: stopHooks, done: make(chan struct{})}
	markCanceled := func() {
		runtime.mu.Lock()
		if !starting.committed {
			starting.canceled = true
			starting.cancel()
		}
		runtime.mu.Unlock()
	}
	starting.stopHooks = append(starting.stopHooks, context.AfterFunc(ctx, markCanceled))
	if requestContext != nil {
		starting.stopHooks = append(starting.stopHooks, context.AfterFunc(requestContext, markCanceled))
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed {
		stopPendingStart(starting)
		return nil, nil, ErrClosed
	}
	if runtime.starts[sessionID] != nil || runtime.clients[sessionID] != nil || runtime.servers[sessionID] != nil || runtime.peers[sessionID] != nil {
		stopPendingStart(starting)
		return nil, nil, ErrEndpointNotRunning
	}
	if err := ctx.Err(); err != nil {
		stopPendingStart(starting)
		return nil, nil, err
	}
	if requestContext != nil && requestContext.Err() != nil {
		stopPendingStart(starting)
		return nil, nil, requestContext.Err()
	}
	runtime.starts[sessionID] = starting
	runtime.startWG.Add(1)
	return ctx, starting, nil
}

func (runtime *Runtime) endStart(sessionID string, starting *pendingStart) {
	stopPendingStart(starting)
	runtime.mu.Lock()
	if runtime.starts[sessionID] == starting {
		delete(runtime.starts, sessionID)
	}
	close(starting.done)
	runtime.mu.Unlock()
	runtime.startWG.Done()
}

func stopPendingStart(starting *pendingStart) {
	for _, stopHook := range starting.stopHooks {
		stopHook()
	}
	starting.cancel()
}

func (runtime *Runtime) validateStartCommitLocked(sessionID string, starting *pendingStart) error {
	if runtime.closed {
		return ErrClosed
	}
	if runtime.starts[sessionID] != starting {
		return ErrEndpointNotRunning
	}
	if starting.request != nil && starting.request.Err() != nil {
		starting.canceled = true
		return starting.request.Err()
	}
	if starting.ctx.Err() != nil {
		starting.canceled = true
		return starting.ctx.Err()
	}
	if starting.canceled {
		return context.Canceled
	}
	starting.committed = true
	return nil
}
