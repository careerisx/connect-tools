package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"connect-tools/internal/framing"
	"connect-tools/internal/profile"
	"connect-tools/internal/records"
	"connect-tools/internal/session"
	cttcp "connect-tools/internal/tcp"
)

// ErrProfileNotTCP indicates that a TCP start was requested for another transport.
var ErrProfileNotTCP = errors.New("profile is not a TCP profile")

// TCPService coordinates profile-backed TCP sessions with the process-owned
// network runtime. It contains no protobuf or HTTP concerns.
type TCPService struct {
	profiles profile.Store
	sessions *SessionService
	runtime  *cttcp.Runtime
}

// NewTCPService creates the TCP application service.
func NewTCPService(profiles profile.Store, sessions *SessionService, runtime *cttcp.Runtime) *TCPService {
	return &TCPService{profiles: profiles, sessions: sessions, runtime: runtime}
}

// Start creates and starts a TCP client or server from a saved profile. A
// failed network startup leaves the created session in Error for diagnostics.
func (service *TCPService) Start(ctx context.Context, profileID, displayName string) (session.Session, error) {
	if service == nil || service.profiles == nil || service.sessions == nil || service.runtime == nil {
		return session.Session{}, fmt.Errorf("TCP service is not initialized")
	}
	saved, err := service.profiles.Get(ctx, profileID)
	if err != nil {
		return session.Session{}, err
	}
	if err := saved.Validate(); err != nil {
		return session.Session{}, err
	}
	if saved.Type != profile.TypeTCPClient && saved.Type != profile.TypeTCPServer {
		return session.Session{}, ErrProfileNotTCP
	}
	created, err := service.sessions.Create(ctx, profileID, displayName)
	if err != nil {
		return session.Session{}, err
	}
	switch saved.Type {
	case profile.TypeTCPClient:
		return service.runtime.StartClient(ctx, created.ID, tcpClientConfig(*saved.TCPClient))
	case profile.TypeTCPServer:
		return service.runtime.StartServer(ctx, created.ID, tcpServerConfig(*saved.TCPServer))
	default:
		return created, ErrProfileNotTCP
	}
}

// StartProfile is an explicit alias for Start for callers that expose multiple
// transport-specific start operations.
func (service *TCPService) StartProfile(ctx context.Context, profileID, displayName string) (session.Session, error) {
	return service.Start(ctx, profileID, displayName)
}

// Stop stops a TCP client, server, or peer session.
func (service *TCPService) Stop(ctx context.Context, sessionID string) (session.Session, error) {
	if service == nil || service.runtime == nil {
		return session.Session{}, fmt.Errorf("TCP service is not initialized")
	}
	return service.runtime.Stop(ctx, sessionID)
}

// Send queues bytes to a TCP client or peer, or broadcasts them to a server's
// current peer snapshot. Actor must be supplied by the calling entry point.
func (service *TCPService) Send(ctx context.Context, targetSessionID string, payload []byte, actor records.Actor) (cttcp.SendResult, error) {
	if service == nil || service.runtime == nil {
		return cttcp.SendResult{}, fmt.Errorf("TCP service is not initialized")
	}
	return service.runtime.Send(ctx, targetSessionID, payload, actor)
}

// ListPeers returns current peers of one TCP server in deterministic order.
func (service *TCPService) ListPeers(ctx context.Context, serverSessionID string) ([]session.Session, error) {
	if service == nil || service.runtime == nil {
		return nil, fmt.Errorf("TCP service is not initialized")
	}
	return service.runtime.ListPeers(ctx, serverSessionID)
}

func tcpClientConfig(saved profile.TCPClient) cttcp.ClientConfig {
	return cttcp.ClientConfig{
		Host: saved.Host, Port: saved.Port,
		ConnectTimeout: time.Duration(saved.ConnectTimeoutMS) * time.Millisecond,
		ReadTimeout:    time.Duration(saved.ReadTimeoutMS) * time.Millisecond,
		WriteTimeout:   time.Duration(saved.WriteTimeoutMS) * time.Millisecond,
		KeepAlive:      saved.KeepAlive, KeepAlivePeriod: time.Duration(saved.KeepAlivePeriodMS) * time.Millisecond,
		NoDelay: saved.NoDelay, Framing: tcpFramingConfig(saved.Framing), AutoReconnect: saved.AutoReconnect,
	}
}

func tcpServerConfig(saved profile.TCPServer) cttcp.ServerConfig {
	return cttcp.ServerConfig{
		BindHost: saved.BindHost, Port: saved.Port, MaxConnections: saved.MaxConnections,
		KeepAlive: saved.KeepAlive, NoDelay: saved.NoDelay, Framing: tcpFramingConfig(saved.Framing),
	}
}

func tcpFramingConfig(saved profile.FramingConfig) framing.Config {
	config := framing.Config{
		FixedLength: saved.FixedLength, Delimiter: append([]byte(nil), saved.Delimiter...),
		IncludeDelimiter: saved.IncludeDelimiter, MaxFrameSize: profile.MaxFrameBytes,
	}
	switch saved.Mode {
	case profile.FrameModeRawRead:
		config.Mode = framing.ModeRawRead
	case profile.FrameModeFixedLength:
		config.Mode = framing.ModeFixedLength
	case profile.FrameModeDelimiter:
		config.Mode = framing.ModeDelimiter
	}
	return config
}
