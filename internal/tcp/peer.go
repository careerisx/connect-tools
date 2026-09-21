package tcp

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"sync"
	"time"

	"connect-tools/internal/framing"
	"connect-tools/internal/session"
)

type peerEndpoint struct {
	id       string
	serverID string
	ctx      context.Context
	cancel   context.CancelFunc
	conn     *net.TCPConn
	queue    *sendQueue
	done     chan struct{}
	once     sync.Once
	config   ServerConfig
	snapshot session.Session
}

func (runtime *Runtime) acceptPeer(server *serverEndpoint, connection *net.TCPConn) error {
	runtime.mu.Lock()
	closed := runtime.closed || server.ctx.Err() != nil || runtime.servers[server.id] != server
	atCapacity := len(server.peers) >= server.config.MaxConnections
	runtime.mu.Unlock()
	if closed {
		return ErrClosed
	}
	if atCapacity {
		return ErrResourceExhausted
	}
	if err := configureTCPConn(connection, server.config.KeepAlive, server.config.KeepAlivePeriod, server.config.NoDelay); err != nil {
		return &networkOperationError{operation: "tcp.configure", err: err}
	}
	decoder, err := framing.New(server.config.Framing)
	if err != nil {
		return &networkOperationError{operation: "tcp.decode", err: err}
	}
	id, err := newUUID()
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	peerSession := session.Session{
		ID: id, ParentID: server.id, Kind: session.KindTCPPeer, State: session.StateRunning,
		DisplayName: connection.RemoteAddr().String(), LocalAddress: connection.LocalAddr().String(), RemoteAddress: connection.RemoteAddr().String(),
		CreatedAt: now, UpdatedAt: now,
	}

	runtime.mu.Lock()
	if runtime.closed || server.ctx.Err() != nil || runtime.servers[server.id] != server {
		runtime.mu.Unlock()
		return ErrClosed
	}
	if len(server.peers) >= server.config.MaxConnections {
		runtime.mu.Unlock()
		return ErrResourceExhausted
	}
	if _, err = runtime.sessions.Create(peerSession); err != nil {
		runtime.mu.Unlock()
		return err
	}
	peerContext, cancel := context.WithCancel(server.ctx)
	peer := &peerEndpoint{
		id: id, serverID: server.id, ctx: peerContext, cancel: cancel, conn: connection,
		queue: newSendQueue(server.config.Queue), done: make(chan struct{}), config: server.config, snapshot: peerSession,
	}
	server.peers[id] = peer
	runtime.peers[id] = peer
	runtime.wg.Add(1)
	go runtime.runPeer(peer, decoder)
	runtime.mu.Unlock()
	return nil
}

func (runtime *Runtime) runPeer(peer *peerEndpoint, decoder framing.Decoder) {
	defer runtime.wg.Done()
	err := runtime.runConnection(peer.ctx, peer.conn, decoder, peer.queue, peer.id, peer.id, 0, 0)
	if peer.ctx.Err() == nil {
		operation := failureOperation(err, "tcp.peer")
		retryable := isRetryableNetworkFailure(err)
		runtime.appendNetworkError(peer.id, peer.id, operation, err, retryable)
		runtime.markError(peer.id, operation, err, retryable)
	}
	runtime.cleanupPeer(peer)
}

func (runtime *Runtime) cleanupPeer(peer *peerEndpoint) {
	peer.once.Do(func() {
		peer.cancel()
		peer.queue.Close()
		_ = peer.conn.Close()
		runtime.finishStopped(peer.id)
		_, _ = runtime.sessions.Remove(peer.id)
		runtime.mu.Lock()
		delete(runtime.peers, peer.id)
		if server := runtime.servers[peer.serverID]; server != nil {
			delete(server.peers, peer.id)
		}
		runtime.mu.Unlock()
		close(peer.done)
	})
}

func (runtime *Runtime) stopPeer(ctx context.Context, peer *peerEndpoint) (session.Session, error) {
	snapshot, err := runtime.sessions.Get(peer.id)
	if err != nil {
		snapshot = peer.snapshot.Clone()
	}
	peer.queue.Close()
	peer.cancel()
	_ = peer.conn.Close()
	runtime.beginStopping(peer.id)
	select {
	case <-peer.done:
		snapshot.State = session.StateStopped
		snapshot.UpdatedAt = time.Now().UTC()
		return snapshot, nil
	case <-ctx.Done():
		return session.Session{}, ctx.Err()
	}
}

func newUUID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate peer UUID: %w", err)
	}
	raw[6] = raw[6]&0x0f | 0x40
	raw[8] = raw[8]&0x3f | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:16]), nil
}
