package api

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"

	"connectrpc.com/connect"
)

const (
	// BrowserSessionCookieName is the cookie used for browser RPC sessions.
	BrowserSessionCookieName = "connect_tools_session"

	createBrowserLoginProcedure = "/connecttools.v1.CoreService/CreateBrowserLogin"
)

// ErrBrowserLoginCapacity indicates that the pending browser-login ticket
// limit has been reached.
var ErrBrowserLoginCapacity = errors.New("browser login ticket capacity reached")

// SessionValidator validates an opaque browser session identifier.
type SessionValidator interface {
	ValidBrowserSession(sessionID string) bool
}

// CredentialKind identifies the trusted authentication mechanism used for an
// RPC request.
type CredentialKind uint8

const (
	// CredentialBearer identifies a request authenticated by the root bearer
	// token.
	CredentialBearer CredentialKind = iota + 1
	// CredentialBrowserSession identifies a request authenticated by the
	// browser session cookie.
	CredentialBrowserSession
)

type credentialContextKey struct{}

// CredentialFromContext reports which validated authentication mechanism was
// used for the request.
func CredentialFromContext(ctx context.Context) (CredentialKind, bool) {
	credential, ok := ctx.Value(credentialContextKey{}).(CredentialKind)
	return credential, ok
}

// ValidBearer compares a Bearer authorization header with the expected token
// in constant time.
func ValidBearer(header, expected string) bool {
	const prefix = "Bearer "
	if expected == "" || !strings.HasPrefix(header, prefix) {
		return false
	}
	actual := strings.TrimSpace(strings.TrimPrefix(header, prefix))
	return subtle.ConstantTimeCompare([]byte(actual), []byte(expected)) == 1
}

// NewAuthInterceptor creates a Connect interceptor that accepts exactly one
// valid bearer token or browser session cookie. CreateBrowserLogin only accepts
// bearer authentication. Browser sessions require an exact expectedOrigin;
// bearer requests may omit Origin, but an Origin they provide must match.
func NewAuthInterceptor(token, expectedOrigin string, sessions SessionValidator) connect.Interceptor {
	return &authInterceptor{
		token:          token,
		expectedOrigin: expectedOrigin,
		sessions:       sessions,
	}
}

type authInterceptor struct {
	token          string
	expectedOrigin string
	sessions       SessionValidator
}

func (i *authInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, request connect.AnyRequest) (connect.AnyResponse, error) {
		ctx, err := i.authorize(ctx, request.Spec().Procedure, request.Header())
		if err != nil {
			return nil, err
		}
		return next(ctx, request)
	}
}

func (i *authInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (i *authInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, connection connect.StreamingHandlerConn) error {
		ctx, err := i.authorize(ctx, connection.Spec().Procedure, connection.RequestHeader())
		if err != nil {
			return err
		}
		return next(ctx, connection)
	}
}

func (i *authInterceptor) authorize(ctx context.Context, procedure string, header http.Header) (context.Context, error) {
	authorizationValues := header.Values("Authorization")
	bearerPresent := len(authorizationValues) > 0
	sessionID, sessionPresent := browserSession(header)
	if bearerPresent == sessionPresent {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("exactly one authentication mechanism is required"))
	}

	var credential CredentialKind
	switch {
	case bearerPresent:
		if len(authorizationValues) != 1 || !ValidBearer(authorizationValues[0], i.token) {
			return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid bearer token"))
		}
		credential = CredentialBearer
	case i.sessions == nil || !i.sessions.ValidBrowserSession(sessionID):
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid browser session"))
	default:
		credential = CredentialBrowserSession
	}
	if err := validateOrigin(header, i.expectedOrigin, credential == CredentialBrowserSession); err != nil {
		return nil, err
	}

	if procedure == createBrowserLoginProcedure && credential != CredentialBearer {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("bearer authentication is required"))
	}

	return context.WithValue(ctx, credentialContextKey{}, credential), nil
}

func validateOrigin(header http.Header, expected string, required bool) error {
	origins := header.Values("Origin")
	if len(origins) == 0 && !required {
		return nil
	}
	if len(origins) != 1 || origins[0] != expected {
		return connect.NewError(connect.CodePermissionDenied, errors.New("origin is not allowed"))
	}
	return nil
}

func browserSession(header http.Header) (string, bool) {
	request := http.Request{Header: header}
	var sessionID string
	found := false
	for _, cookie := range request.Cookies() {
		if cookie.Name != BrowserSessionCookieName {
			continue
		}
		if found {
			return "", true
		}
		sessionID = cookie.Value
		found = true
	}
	return sessionID, found
}
