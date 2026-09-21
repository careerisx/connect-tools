package api

import (
	"context"
	"errors"
	"net"

	"connect-tools/internal/app"
	"connect-tools/internal/convert"
	"connect-tools/internal/mqtt"
	"connect-tools/internal/profile"
	"connect-tools/internal/records"
	"connect-tools/internal/secrets"
	"connect-tools/internal/session"
	cttcp "connect-tools/internal/tcp"
	"connectrpc.com/connect"
)

func rpcError(err error) error {
	if err == nil {
		return nil
	}
	if connectErr := new(connect.Error); errors.As(err, &connectErr) {
		return connectErr
	}
	code, message := classifyDomainError(err)
	return connect.NewError(code, errors.New(message))
}

func invalidArgument(message string) error {
	return connect.NewError(connect.CodeInvalidArgument, errors.New(message))
}

func classifyDomainError(err error) (connect.Code, string) {
	var networkError net.Error
	if errors.As(err, &networkError) {
		return connect.CodeUnavailable, "network operation failed"
	}
	switch {
	case errors.Is(err, context.Canceled):
		return connect.CodeCanceled, "operation canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return connect.CodeDeadlineExceeded, "operation timed out"
	case errors.Is(err, profile.ErrNotFound), errors.Is(err, session.ErrNotFound), errors.Is(err, cttcp.ErrEndpointNotFound), errors.Is(err, secrets.ErrNotFound):
		return connect.CodeNotFound, "requested resource was not found"
	case errors.Is(err, profile.ErrDuplicateName), errors.Is(err, session.ErrDuplicateID):
		return connect.CodeAlreadyExists, "resource already exists"
	case errors.Is(err, session.ErrIllegalTransition), errors.Is(err, session.ErrSessionActive), errors.Is(err, session.ErrSessionHasPeers), errors.Is(err, cttcp.ErrEndpointNotRunning), errors.Is(err, app.ErrProfileNotTCP), errors.Is(err, app.ErrProfileNotMQTT), errors.Is(err, mqtt.ErrClientNotConnected):
		return connect.CodeFailedPrecondition, "resource is not in the required state"
	case errors.Is(err, records.ErrResourceExhausted), errors.Is(err, cttcp.ErrResourceExhausted), errors.Is(err, cttcp.ErrPayloadTooLarge):
		return connect.CodeResourceExhausted, "resource limit exceeded"
	case errors.Is(err, cttcp.ErrClosed), errors.Is(err, secrets.ErrUnavailable), errors.Is(err, mqtt.ErrRuntimeClosed):
		return connect.CodeUnavailable, "service is unavailable"
	case errors.Is(err, profile.ErrInvalidProfile), errors.Is(err, profile.ErrInvalidID), errors.Is(err, profile.ErrInvalidName),
		errors.Is(err, profile.ErrInvalidType), errors.Is(err, profile.ErrInvalidConfiguration), errors.Is(err, profile.ErrInvalidHost),
		errors.Is(err, profile.ErrInvalidPort), errors.Is(err, profile.ErrInvalidTimeout), errors.Is(err, profile.ErrInvalidKeepAlive),
		errors.Is(err, profile.ErrInvalidMaxConnections), errors.Is(err, profile.ErrInvalidFraming), errors.Is(err, profile.ErrFrameTooLarge),
		errors.Is(err, session.ErrInvalidSession), errors.Is(err, session.ErrInvalidParent), errors.Is(err, records.ErrInvalidArgument),
		errors.Is(err, cttcp.ErrInvalidConfig), errors.Is(err, mqtt.ErrInvalidConfig), errors.Is(err, mqtt.ErrInvalidOperation), errors.Is(err, mqtt.ErrUnsupportedVersion), errors.Is(err, convert.ErrInvalidPayload), errors.Is(err, convert.ErrInvalidEncoding),
		errors.Is(err, convert.ErrInvalidCharset), errors.Is(err, app.ErrInvalidSecretUpdate), errors.Is(err, secrets.ErrInvalidReference):
		return connect.CodeInvalidArgument, "invalid request"
	default:
		return connect.CodeInternal, "internal operation failed"
	}
}

func peerError(err error) (code, message string, retryable bool) {
	if err == nil {
		return "", "", false
	}
	connectCode, safeMessage := classifyDomainError(err)
	retryable = connectCode == connect.CodeResourceExhausted || connectCode == connect.CodeUnavailable || connectCode == connect.CodeDeadlineExceeded
	return connectCode.String(), safeMessage, retryable
}
