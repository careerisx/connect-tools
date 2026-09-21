// Package tcp implements the process-owned TCP client, server, and accepted
// peer runtimes used by every application entry point.
package tcp

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"connect-tools/internal/framing"
	"connect-tools/internal/records"
)

const (
	// MaxPayloadBytes is the largest payload accepted by one send operation.
	MaxPayloadBytes = 16 << 20
	// DefaultQueueMessages bounds queued sends for one physical connection.
	DefaultQueueMessages = 128
	// DefaultQueueBytes bounds queued payload storage for one connection.
	DefaultQueueBytes = 32 << 20
	// DefaultReadBufferBytes is the socket read buffer used by read loops.
	DefaultReadBufferBytes = 64 << 10
	// MaxServerConnections is the hard safety limit for one TCP listener.
	MaxServerConnections = 10_000
)

var (
	// ErrClosed indicates that the TCP runtime no longer accepts operations.
	ErrClosed = errors.New("TCP runtime is closed")
	// ErrInvalidConfig indicates invalid TCP or framing configuration.
	ErrInvalidConfig = errors.New("invalid TCP configuration")
	// ErrPayloadTooLarge indicates a send above MaxPayloadBytes.
	ErrPayloadTooLarge = errors.New("TCP payload exceeds maximum size")
	// ErrResourceExhausted indicates an endpoint send queue without capacity.
	ErrResourceExhausted = errors.New("TCP send queue is full")
	// ErrEndpointNotFound indicates that no active runtime endpoint owns a session.
	ErrEndpointNotFound = errors.New("TCP endpoint not found")
	// ErrEndpointNotRunning indicates that a known session cannot currently send.
	ErrEndpointNotRunning = errors.New("TCP endpoint is not running")
)

// QueueLimits bounds the number and total bytes of pending sends per physical
// connection. Zero values select defaults.
type QueueLimits struct {
	Messages int
	Bytes    int
}

func (limits QueueLimits) normalized() (QueueLimits, error) {
	if limits.Messages == 0 {
		limits.Messages = DefaultQueueMessages
	}
	if limits.Bytes == 0 {
		limits.Bytes = DefaultQueueBytes
	}
	if limits.Messages < 1 || limits.Bytes < 1 {
		return QueueLimits{}, fmt.Errorf("%w: queue limits must be positive", ErrInvalidConfig)
	}
	return limits, nil
}

// ClientConfig configures one TCP client runtime session.
type ClientConfig struct {
	Host            string
	Port            int
	ConnectTimeout  time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	KeepAlive       bool
	KeepAlivePeriod time.Duration
	NoDelay         bool
	Framing         framing.Config
	AutoReconnect   bool
	Reconnect       Backoff
	Queue           QueueLimits
}

// ServerConfig configures one TCP listener and all peers accepted by it.
type ServerConfig struct {
	BindHost        string
	Port            int
	MaxConnections  int
	KeepAlive       bool
	KeepAlivePeriod time.Duration
	NoDelay         bool
	Framing         framing.Config
	Queue           QueueLimits
}

// PeerSendResult describes whether one peer accepted a broadcast payload.
type PeerSendResult struct {
	PeerSessionID string
	Accepted      bool
	Error         error
}

// SendResult describes one directed send or server broadcast. PeerResults are
// sorted by peer session ID.
type SendResult struct {
	Accepted    bool
	PeerResults []PeerSendResult
}

func (config ClientConfig) validate() (ClientConfig, error) {
	config.Host = strings.TrimSpace(config.Host)
	if config.Host == "" || config.Port < 1 || config.Port > 65535 {
		return ClientConfig{}, fmt.Errorf("%w: invalid client address", ErrInvalidConfig)
	}
	if config.ConnectTimeout < 0 || config.ReadTimeout < 0 || config.WriteTimeout < 0 || config.KeepAlivePeriod < 0 {
		return ClientConfig{}, fmt.Errorf("%w: timeout must not be negative", ErrInvalidConfig)
	}
	if _, err := framing.New(config.Framing); err != nil {
		return ClientConfig{}, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	if err := config.Reconnect.validate(); err != nil {
		return ClientConfig{}, err
	}
	queue, err := config.Queue.normalized()
	if err != nil {
		return ClientConfig{}, err
	}
	config.Queue = queue
	config.Reconnect = config.Reconnect.normalized()
	return config, nil
}

func (config ServerConfig) validate() (ServerConfig, error) {
	config.BindHost = strings.TrimSpace(config.BindHost)
	if config.BindHost == "" || config.Port < 0 || config.Port > 65535 {
		return ServerConfig{}, fmt.Errorf("%w: invalid server address", ErrInvalidConfig)
	}
	if config.MaxConnections < 1 || config.MaxConnections > MaxServerConnections || config.KeepAlivePeriod < 0 {
		return ServerConfig{}, fmt.Errorf("%w: invalid server limits", ErrInvalidConfig)
	}
	if _, err := framing.New(config.Framing); err != nil {
		return ServerConfig{}, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	queue, err := config.Queue.normalized()
	if err != nil {
		return ServerConfig{}, err
	}
	config.Queue = queue
	return config, nil
}

func address(host string, port int) string {
	return net.JoinHostPort(host, fmt.Sprintf("%d", port))
}

type queuedSend struct {
	payload []byte
	actor   records.Actor
}
