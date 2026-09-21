package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"connect-tools/gen/go/connecttools/v1/connecttoolsv1connect"
	"connect-tools/internal/api"
	"connect-tools/internal/app"
	ctmcp "connect-tools/internal/mcp"
	"connect-tools/internal/records"
	"connect-tools/internal/secrets"
	"connect-tools/internal/web"
	"connectrpc.com/connect"
)

const (
	// DefaultAddress is the fixed loopback address used by Connect Tools.
	DefaultAddress = "127.0.0.1:8765"

	readHeaderTimeout = 5 * time.Second
	readTimeout       = 30 * time.Second
	idleTimeout       = 60 * time.Second
	shutdownTimeout   = 5 * time.Second
	maxHeaderBytes    = 32 << 10
	// maxConnectRequestBodyBytes accommodates a future 16 MiB application
	// payload plus Connect framing and JSON/base64 encoding overhead.
	maxConnectRequestBodyBytes int64 = 24 << 20
)

// ErrServerAlreadyRun indicates that Run was called more than once.
var ErrServerAlreadyRun = errors.New("server can only be run once")

// Options contains the dependencies and local transport settings for Server.
type Options struct {
	Address   string
	Token     string
	Core      *app.Core
	Profiles  *app.ProfileService
	Sessions  *app.SessionService
	TCP       *app.TCPService
	MQTT      *app.MQTTService
	Records   *records.Store
	Secrets   secrets.Store
	WebAssets fs.FS
}

// Server hosts the local Connect API, browser login exchange, health endpoint,
// and embedded web application.
type Server struct {
	address string
	core    *app.Core
	login   *loginManager
	handler http.Handler
	http    *http.Server

	shutdown     chan struct{}
	shutdownOnce sync.Once
	runMu        sync.Mutex
	runStarted   bool
}

// New validates options and constructs a local-only HTTP server.
func New(options Options) (*Server, error) {
	address := options.Address
	if address == "" {
		address = DefaultAddress
	}
	if err := validateAddress(address); err != nil {
		return nil, err
	}
	if options.Token == "" {
		return nil, errors.New("server token is required")
	}
	if options.Core == nil {
		return nil, errors.New("server core is required")
	}

	assets := options.WebAssets
	if assets == nil {
		assets = web.Assets()
	}

	login := newLoginManager(address)
	server := &Server{
		address:  address,
		core:     options.Core,
		login:    login,
		shutdown: make(chan struct{}),
	}
	coreHandler := api.NewCoreHandler(options.Core, login, server.signalShutdown)
	authInterceptor := api.NewAuthInterceptor(options.Token, "http://"+address, login)
	connectPath, connectHandler := connecttoolsv1connect.NewCoreServiceHandler(
		coreHandler,
		connect.WithInterceptors(authInterceptor),
	)

	mux := http.NewServeMux()
	mux.Handle("GET /healthz", healthHandler())
	mux.Handle("GET /login", login.handler())
	mux.Handle(connectPath, limitRequestBody(maxConnectRequestBodyBytes, connectHandler))
	if options.Profiles != nil {
		path, handler := connecttoolsv1connect.NewProfileServiceHandler(
			api.NewProfileHandler(options.Profiles),
			connect.WithInterceptors(authInterceptor),
		)
		mux.Handle(path, limitRequestBody(maxConnectRequestBodyBytes, handler))
	}
	if options.Sessions != nil || options.TCP != nil {
		if options.Sessions == nil || options.TCP == nil {
			return nil, errors.New("server session and TCP services must be provided together")
		}
		path, handler := connecttoolsv1connect.NewSessionServiceHandler(
			api.NewSessionHandlerWithMQTT(options.Sessions, options.TCP, options.MQTT),
			connect.WithInterceptors(authInterceptor),
		)
		mux.Handle(path, limitRequestBody(maxConnectRequestBodyBytes, handler))
		path, handler = connecttoolsv1connect.NewTCPServiceHandler(
			api.NewTCPHandler(options.TCP),
			connect.WithInterceptors(authInterceptor),
		)
		mux.Handle(path, limitRequestBody(maxConnectRequestBodyBytes, handler))
	}
	if options.MQTT != nil {
		path, handler := connecttoolsv1connect.NewMQTTServiceHandler(api.NewMQTTHandler(options.MQTT), connect.WithInterceptors(authInterceptor))
		mux.Handle(path, limitRequestBody(maxConnectRequestBodyBytes, handler))
	}
	if options.Records != nil {
		path, handler := connecttoolsv1connect.NewRecordServiceHandler(
			api.NewRecordHandler(options.Records),
			connect.WithInterceptors(authInterceptor),
		)
		mux.Handle(path, limitRequestBody(maxConnectRequestBodyBytes, handler))
	}
	if options.Profiles != nil && options.Sessions != nil && options.TCP != nil && options.MQTT != nil && options.Records != nil && options.Secrets != nil {
		mcpHandler, err := ctmcp.NewHandler(options.Token, ctmcp.Dependencies{Core: options.Core, Profiles: options.Profiles, Sessions: options.Sessions, TCP: options.TCP, MQTT: options.MQTT, Records: options.Records, Secrets: options.Secrets})
		if err != nil {
			return nil, err
		}
		mux.Handle("/mcp", mcpHandler)
	}
	mux.Handle("/", login.browserOnly(web.NewHandler(assets)))

	server.handler = mux
	server.http = &http.Server{
		Addr:              address,
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      0,
		IdleTimeout:       idleTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
	}
	return server, nil
}

// Handler returns the fully composed HTTP handler for in-process serving or
// transport-level verification.
func (s *Server) Handler() http.Handler {
	return s.handler
}

// CreateBrowserLoginURL creates a one-time browser login URL.
func (s *Server) CreateBrowserLoginURL() (string, error) {
	return s.login.CreateBrowserLoginURL()
}

// RequestShutdown idempotently marks the core as stopping and asks Run to
// gracefully stop the HTTP server.
func (s *Server) RequestShutdown() {
	s.core.Shutdown()
	s.signalShutdown()
}

// Run listens on the configured loopback address until the context is
// canceled, RequestShutdown is called, or the HTTP server fails.
func (s *Server) Run(ctx context.Context) error {
	return s.RunWithLifecycle(ctx, nil, nil)
}

// RunWithLifecycle listens on the configured loopback address and brackets
// the listener ownership with callbacks. onReady runs after listening and
// serving have started. beforeShutdown runs at most once before the listener
// is released, including when onReady fails or Serve exits unexpectedly.
// Callback panics become errors after listener and goroutine cleanup completes.
func (s *Server) RunWithLifecycle(ctx context.Context, onReady func() error, beforeShutdown func()) error {
	var callback func() error
	if beforeShutdown != nil {
		callback = func() error {
			beforeShutdown()
			return nil
		}
	}
	return s.RunWithLifecycleError(ctx, onReady, callback)
}

// RunWithLifecycleError is RunWithLifecycle with an error-returning shutdown
// callback. Callback errors are joined with HTTP shutdown and serve errors.
func (s *Server) RunWithLifecycleError(ctx context.Context, onReady func() error, beforeShutdown func() error) error {
	if ctx == nil {
		ctx = context.Background()
	}

	s.runMu.Lock()
	if s.runStarted {
		s.runMu.Unlock()
		return ErrServerAlreadyRun
	}
	s.runStarted = true
	s.runMu.Unlock()

	select {
	case <-ctx.Done():
		s.RequestShutdown()
		return nil
	case <-s.shutdown:
		return nil
	default:
	}

	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.address, err)
	}
	heldListener := &lifecycleListener{Listener: listener}
	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- s.http.Serve(heldListener)
	}()

	var beforeShutdownOnce sync.Once
	var beforeShutdownErr error
	runBeforeShutdown := func() {
		if beforeShutdown == nil {
			return
		}
		beforeShutdownOnce.Do(func() {
			beforeShutdownErr = invokeBeforeShutdown(beforeShutdown)
		})
	}

	var readyErr error
	var serveErr error
	serveDone := false
	select {
	case serveErr = <-serveErrors:
		serveDone = true
	case <-ctx.Done():
		s.RequestShutdown()
	case <-s.shutdown:
	default:
		if onReady != nil {
			readyErr = invokeOnReady(onReady)
		}
	}

	if readyErr == nil && !serveDone {
		select {
		case serveErr = <-serveErrors:
			serveDone = true
		case <-ctx.Done():
			s.RequestShutdown()
		case <-s.shutdown:
		}
	}

	// End event streams before waiting for active HTTP handlers. This also
	// covers startup callback failures and unexpected Serve termination.
	s.core.Shutdown()

	// Keep ownership of the listener until lifecycle state has been removed.
	// This prevents a successor from publishing its runtime metadata before the
	// previous instance has completed beforeShutdown.
	runBeforeShutdown()
	heldListener.allowClose()

	shutdownContext, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	shutdownErr := s.http.Shutdown(shutdownContext)
	cancel()
	if shutdownErr != nil {
		_ = s.http.Close()
	}
	releaseErr := heldListener.release()
	if !serveDone {
		serveErr = <-serveErrors
	}

	resultErr := errors.Join(readyErr, beforeShutdownErr)
	if shutdownErr != nil {
		resultErr = errors.Join(resultErr, fmt.Errorf("shut down HTTP server: %w", shutdownErr))
	}
	if releaseErr != nil {
		resultErr = errors.Join(resultErr, fmt.Errorf("release server listener: %w", releaseErr))
	}
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		resultErr = errors.Join(resultErr, fmt.Errorf("serve HTTP: %w", serveErr))
	}
	return resultErr
}

func invokeOnReady(callback func() error) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("onReady callback panicked")
		}
	}()
	if err := callback(); err != nil {
		return fmt.Errorf("onReady callback: %w", err)
	}
	return nil
}

func invokeBeforeShutdown(callback func() error) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("beforeShutdown callback panicked")
		}
	}()
	if err := callback(); err != nil {
		return fmt.Errorf("beforeShutdown callback: %w", err)
	}
	return nil
}

// lifecycleListener defers the listener close requested by http.Server until
// lifecycle metadata has been removed. http.Server wraps and closes the
// listener internally even when Serve returns unexpectedly.
type lifecycleListener struct {
	net.Listener

	mu           sync.Mutex
	closeAllowed bool
	closed       bool
}

func (l *lifecycleListener) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	if !l.closeAllowed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	l.mu.Unlock()
	return l.Listener.Close()
}

func (l *lifecycleListener) allowClose() {
	l.mu.Lock()
	l.closeAllowed = true
	l.mu.Unlock()
}

func (l *lifecycleListener) release() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closeAllowed = true
	l.closed = true
	l.mu.Unlock()
	return l.Listener.Close()
}

func (s *Server) signalShutdown() {
	s.shutdownOnce.Do(func() {
		close(s.shutdown)
	})
}

func validateAddress(address string) error {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid server address %q: %w", address, err)
	}
	if host != "127.0.0.1" {
		return fmt.Errorf("server address must use 127.0.0.1")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("invalid server port %q", portText)
	}
	return nil
}

func healthHandler() http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json; charset=utf-8")
		response.Header().Set("Cache-Control", "no-store")
		response.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(response, "{\"status\":\"ok\"}\n")
	})
}

func limitRequestBody(maxBytes int64, next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Body != nil {
			request.Body = http.MaxBytesReader(response, request.Body, maxBytes)
		}
		if request.ContentLength > maxBytes {
			http.Error(response, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		next.ServeHTTP(response, request)
	})
}
