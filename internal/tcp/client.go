package tcp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"connect-tools/internal/framing"
	"connect-tools/internal/session"
)

type clientEndpoint struct {
	id     string
	config ClientConfig
	ctx    context.Context
	cancel context.CancelFunc
	queue  *sendQueue

	connMu      sync.Mutex
	conn        *net.TCPConn
	cleanup     sync.Once
	cleanupDone chan struct{}
}

// StartClient dials and starts an existing stopped TCP client session. The
// request context controls only initial startup; the resulting connection is
// owned by Runtime.
func (runtime *Runtime) StartClient(ctx context.Context, sessionID string, config ClientConfig) (session.Session, error) {
	config, err := config.validate()
	if err != nil {
		return session.Session{}, err
	}
	if err := callContextError(ctx); err != nil {
		return session.Session{}, err
	}
	snapshot, err := runtime.sessions.Get(sessionID)
	if err != nil {
		return session.Session{}, err
	}
	if snapshot.Kind != session.KindTCPClient || snapshot.State != session.StateStopped {
		return session.Session{}, ErrEndpointNotRunning
	}
	runtime.mu.Lock()
	if runtime.closed {
		runtime.mu.Unlock()
		return session.Session{}, ErrClosed
	}
	if runtime.clients[sessionID] != nil {
		runtime.mu.Unlock()
		return session.Session{}, ErrEndpointNotRunning
	}
	runtime.mu.Unlock()

	dialCtx, starting, err := runtime.beginStart(sessionID, ctx, config.ConnectTimeout)
	if err != nil {
		return session.Session{}, err
	}
	defer runtime.endStart(sessionID, starting)
	if _, err = runtime.sessions.Transition(sessionID, session.StateStarting, nil); err != nil {
		return session.Session{}, err
	}
	conn, err := runtime.dialClient(dialCtx, config)
	if err != nil {
		operation := failureOperation(err, "tcp.connect")
		runtime.settleStartFailure(sessionID, operation, err, isRetryableNetworkFailure(err), runtime.startWasCanceled(sessionID, starting, err))
		return runtime.sessionWithError(sessionID, err)
	}

	endpointContext, cancel := context.WithCancel(runtime.ctx)
	endpoint := &clientEndpoint{
		id: sessionID, config: config, ctx: endpointContext, cancel: cancel,
		queue: newSendQueue(config.Queue), conn: conn, cleanupDone: make(chan struct{}),
	}
	running, err := runtime.commitClientStart(sessionID, starting, endpoint, conn)
	if err != nil {
		cancel()
		endpoint.queue.Close()
		_ = conn.Close()
		runtime.settleStartFailure(sessionID, "tcp.start", err, false, runtime.startWasCanceled(sessionID, starting, err))
		return runtime.sessionWithError(sessionID, err)
	}
	return running, nil
}

func (runtime *Runtime) commitClientStart(sessionID string, starting *pendingStart, endpoint *clientEndpoint, connection *net.TCPConn) (session.Session, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if err := runtime.validateStartCommitLocked(sessionID, starting); err != nil {
		return session.Session{}, err
	}
	if runtime.clients[sessionID] != nil {
		return session.Session{}, ErrEndpointNotRunning
	}
	if _, err := runtime.sessions.UpdateAddresses(sessionID, connection.LocalAddr().String(), connection.RemoteAddr().String()); err != nil {
		return session.Session{}, err
	}
	running, err := runtime.sessions.Transition(sessionID, session.StateRunning, nil)
	if err != nil {
		return session.Session{}, err
	}
	runtime.clients[sessionID] = endpoint
	runtime.wg.Add(1)
	go runtime.runClient(endpoint, connection)
	return running, nil
}

func (runtime *Runtime) dialClient(ctx context.Context, config ClientConfig) (*net.TCPConn, error) {
	dialer := net.Dialer{KeepAlive: -1}
	connection, err := dialer.DialContext(ctx, "tcp", address(config.Host, config.Port))
	if err != nil {
		return nil, fmt.Errorf("dial TCP client: %w", err)
	}
	tcpConnection, ok := connection.(*net.TCPConn)
	if !ok {
		_ = connection.Close()
		return nil, fmt.Errorf("%w: dial returned non-TCP connection", ErrInvalidConfig)
	}
	if err := configureTCPConn(tcpConnection, config.KeepAlive, config.KeepAlivePeriod, config.NoDelay); err != nil {
		_ = tcpConnection.Close()
		return nil, &networkOperationError{operation: "tcp.configure", err: err}
	}
	return tcpConnection, nil
}

func (runtime *Runtime) runClient(endpoint *clientEndpoint, connection *net.TCPConn) {
	defer runtime.wg.Done()
	defer runtime.cleanupClient(endpoint)
	for {
		decoder, err := framing.New(endpoint.config.Framing)
		if err != nil {
			runtime.markError(endpoint.id, "tcp.decode", err, false)
			return
		}
		err = runtime.runConnection(endpoint.ctx, connection, decoder, endpoint.queue, endpoint.id, "", endpoint.config.ReadTimeout, endpoint.config.WriteTimeout)
		endpoint.setConnection(nil)
		if endpoint.ctx.Err() != nil {
			return
		}
		operation := failureOperation(err, "tcp.connection")
		retryable := isRetryableNetworkFailure(err)
		runtime.appendNetworkError(endpoint.id, "", operation, err, retryable)
		runtime.markError(endpoint.id, operation, err, retryable)
		if !endpoint.config.AutoReconnect || !retryable {
			return
		}

		for attempt := 0; ; attempt++ {
			if !waitDelay(endpoint.ctx, endpoint.config.Reconnect.Delay(attempt)) {
				return
			}
			if _, err = runtime.sessions.Transition(endpoint.id, session.StateStarting, nil); err != nil {
				if endpoint.ctx.Err() == nil {
					runtime.markError(endpoint.id, "tcp.reconnect", err, false)
				}
				return
			}
			connection, err = runtime.dialOwnedClient(endpoint.ctx, endpoint.config)
			if err != nil {
				if endpoint.ctx.Err() != nil {
					return
				}
				operation := failureOperation(err, "tcp.reconnect")
				retryable := isRetryableNetworkFailure(err)
				runtime.appendNetworkError(endpoint.id, "", operation, err, retryable)
				runtime.markError(endpoint.id, operation, err, retryable)
				if !retryable {
					return
				}
				continue
			}
			endpoint.setConnection(connection)
			if _, err = runtime.sessions.UpdateAddresses(endpoint.id, connection.LocalAddr().String(), connection.RemoteAddr().String()); err != nil {
				_ = connection.Close()
				runtime.markError(endpoint.id, "tcp.reconnect", err, false)
				return
			}
			if _, err = runtime.sessions.Transition(endpoint.id, session.StateRunning, nil); err != nil {
				_ = connection.Close()
				runtime.markError(endpoint.id, "tcp.reconnect", err, false)
				return
			}
			break
		}
	}
}

func (runtime *Runtime) dialOwnedClient(ctx context.Context, config ClientConfig) (*net.TCPConn, error) {
	dialContext := ctx
	cancel := func() {}
	if config.ConnectTimeout > 0 {
		dialContext, cancel = context.WithTimeout(ctx, config.ConnectTimeout)
	}
	defer cancel()
	dialer := net.Dialer{KeepAlive: -1}
	connection, err := dialer.DialContext(dialContext, "tcp", address(config.Host, config.Port))
	if err != nil {
		return nil, fmt.Errorf("dial TCP client: %w", err)
	}
	tcpConnection, ok := connection.(*net.TCPConn)
	if !ok {
		_ = connection.Close()
		return nil, &networkOperationError{operation: "tcp.configure", err: fmt.Errorf("%w: dial returned non-TCP connection", ErrInvalidConfig)}
	}
	if err := configureTCPConn(tcpConnection, config.KeepAlive, config.KeepAlivePeriod, config.NoDelay); err != nil {
		_ = tcpConnection.Close()
		return nil, &networkOperationError{operation: "tcp.configure", err: err}
	}
	return tcpConnection, nil
}

func (runtime *Runtime) stopClient(ctx context.Context, endpoint *clientEndpoint) (session.Session, error) {
	endpoint.queue.Close()
	endpoint.cancel()
	endpoint.closeConnection()
	runtime.beginStopping(endpoint.id)
	select {
	case <-endpoint.cleanupDone:
	case <-ctx.Done():
		return session.Session{}, ctx.Err()
	}
	runtime.finishStopped(endpoint.id)
	return runtime.sessions.Get(endpoint.id)
}

func (runtime *Runtime) cleanupClient(endpoint *clientEndpoint) {
	endpoint.cleanup.Do(func() {
		endpoint.queue.Close()
		endpoint.cancel()
		endpoint.closeConnection()
		runtime.mu.Lock()
		if runtime.clients[endpoint.id] == endpoint {
			delete(runtime.clients, endpoint.id)
		}
		runtime.mu.Unlock()
		close(endpoint.cleanupDone)
	})
}

func (runtime *Runtime) sessionWithError(id string, cause error) (session.Session, error) {
	snapshot, getErr := runtime.sessions.Get(id)
	if getErr != nil {
		return session.Session{}, errors.Join(cause, getErr)
	}
	return snapshot, cause
}

func (endpoint *clientEndpoint) setConnection(connection *net.TCPConn) {
	endpoint.connMu.Lock()
	endpoint.conn = connection
	endpoint.connMu.Unlock()
}

func (endpoint *clientEndpoint) closeConnection() {
	endpoint.connMu.Lock()
	connection := endpoint.conn
	endpoint.conn = nil
	endpoint.connMu.Unlock()
	if connection != nil {
		_ = connection.Close()
	}
}

func waitDelay(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
