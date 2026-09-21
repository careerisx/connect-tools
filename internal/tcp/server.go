package tcp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"connect-tools/internal/records"
	"connect-tools/internal/session"
)

type serverEndpoint struct {
	id          string
	config      ServerConfig
	ctx         context.Context
	cancel      context.CancelFunc
	listener    *net.TCPListener
	peers       map[string]*peerEndpoint // guarded by Runtime.mu
	cleanup     sync.Once
	cleanupDone chan struct{}
}

// StartServer starts a listener for an existing stopped TCP server session.
// Port zero is accepted for temporary runtime listeners.
func (runtime *Runtime) StartServer(ctx context.Context, sessionID string, config ServerConfig) (session.Session, error) {
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
	if snapshot.Kind != session.KindTCPServer || snapshot.State != session.StateStopped {
		return session.Session{}, ErrEndpointNotRunning
	}
	runtime.mu.Lock()
	if runtime.closed {
		runtime.mu.Unlock()
		return session.Session{}, ErrClosed
	}
	if runtime.servers[sessionID] != nil {
		runtime.mu.Unlock()
		return session.Session{}, ErrEndpointNotRunning
	}
	runtime.mu.Unlock()
	listenContext, starting, err := runtime.beginStart(sessionID, ctx, 0)
	if err != nil {
		return session.Session{}, err
	}
	defer runtime.endStart(sessionID, starting)
	if _, err = runtime.sessions.Transition(sessionID, session.StateStarting, nil); err != nil {
		return session.Session{}, err
	}
	listener, err := (&net.ListenConfig{}).Listen(listenContext, "tcp", address(config.BindHost, config.Port))
	if err != nil {
		runtime.settleStartFailure(sessionID, "tcp.listen", err, isRetryableNetworkFailure(err), runtime.startWasCanceled(sessionID, starting, err))
		return runtime.sessionWithError(sessionID, fmt.Errorf("listen TCP server: %w", err))
	}
	tcpListener, ok := listener.(*net.TCPListener)
	if !ok {
		_ = listener.Close()
		err = fmt.Errorf("%w: listen returned non-TCP listener", ErrInvalidConfig)
		runtime.settleStartFailure(sessionID, "tcp.listen", err, false, runtime.startWasCanceled(sessionID, starting, err))
		return runtime.sessionWithError(sessionID, err)
	}
	endpointContext, cancel := context.WithCancel(runtime.ctx)
	server := &serverEndpoint{id: sessionID, config: config, ctx: endpointContext, cancel: cancel, listener: tcpListener, peers: make(map[string]*peerEndpoint), cleanupDone: make(chan struct{})}
	running, err := runtime.commitServerStart(sessionID, starting, server, tcpListener)
	if err != nil {
		cancel()
		_ = tcpListener.Close()
		runtime.settleStartFailure(sessionID, "tcp.start", err, false, runtime.startWasCanceled(sessionID, starting, err))
		return runtime.sessionWithError(sessionID, err)
	}
	return running, nil
}

func (runtime *Runtime) commitServerStart(sessionID string, starting *pendingStart, server *serverEndpoint, listener *net.TCPListener) (session.Session, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if err := runtime.validateStartCommitLocked(sessionID, starting); err != nil {
		return session.Session{}, err
	}
	if runtime.servers[sessionID] != nil {
		return session.Session{}, ErrEndpointNotRunning
	}
	if _, err := runtime.sessions.UpdateAddresses(sessionID, listener.Addr().String(), ""); err != nil {
		return session.Session{}, err
	}
	running, err := runtime.sessions.Transition(sessionID, session.StateRunning, nil)
	if err != nil {
		return session.Session{}, err
	}
	runtime.servers[sessionID] = server
	runtime.wg.Add(1)
	go runtime.runServer(server)
	return running, nil
}

func (runtime *Runtime) runServer(server *serverEndpoint) {
	defer runtime.wg.Done()
	defer runtime.cleanupServer(server)
	for {
		connection, err := server.listener.AcceptTCP()
		if err != nil {
			if server.ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			retryable := isRetryableNetworkFailure(err)
			runtime.appendNetworkError(server.id, "", "tcp.accept", err, retryable)
			if networkError, ok := err.(net.Error); ok && networkError.Temporary() {
				continue
			}
			runtime.markError(server.id, "tcp.accept", err, retryable)
			return
		}
		if err := runtime.acceptPeer(server, connection); err != nil {
			_ = connection.Close()
			if errors.Is(err, ErrClosed) && server.ctx.Err() != nil {
				return
			}
			operation := failureOperation(err, "tcp.accept")
			code := "peer_rejected"
			if errors.Is(err, ErrResourceExhausted) {
				code = "max_connections"
			}
			retryable := isRetryableNetworkFailure(err)
			_, _ = runtime.records.Append(records.Record{
				SessionID: server.id, Direction: records.DirectionInternal, Actor: records.ActorSystem,
				Error: &records.RecordError{Code: code, Message: err.Error(), Operation: operation, Retryable: retryable, Timestamp: time.Now().UTC()},
			})
		}
	}
}

func (runtime *Runtime) stopServer(ctx context.Context, server *serverEndpoint) (session.Session, error) {
	server.cancel()
	_ = server.listener.Close()
	_ = runtime.cancelServerPeers(server)
	runtime.beginStopping(server.id)
	select {
	case <-server.cleanupDone:
	case <-ctx.Done():
		return session.Session{}, ctx.Err()
	}
	runtime.finishStopped(server.id)
	return runtime.sessions.Get(server.id)
}

func (runtime *Runtime) cleanupServer(server *serverEndpoint) {
	server.cleanup.Do(func() {
		server.cancel()
		_ = server.listener.Close()
		peers := runtime.cancelServerPeers(server)
		for _, peer := range peers {
			<-peer.done
		}
		runtime.mu.Lock()
		if runtime.servers[server.id] == server {
			delete(runtime.servers, server.id)
		}
		runtime.mu.Unlock()
		close(server.cleanupDone)
	})
}

func (runtime *Runtime) cancelServerPeers(server *serverEndpoint) []*peerEndpoint {
	runtime.mu.Lock()
	peers := make([]*peerEndpoint, 0, len(server.peers))
	for _, peer := range server.peers {
		peers = append(peers, peer)
	}
	runtime.mu.Unlock()
	for _, peer := range peers {
		peer.cancel()
		peer.queue.Close()
		_ = peer.conn.Close()
	}
	return peers
}
