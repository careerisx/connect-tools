package api

import (
	"context"
	"errors"

	"connect-tools/gen/go/connecttools/v1"
	"connect-tools/internal/app"
	"connect-tools/internal/records"
	"connect-tools/internal/session"
	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// BrowserLoginIssuer creates a one-time browser login URL.
type BrowserLoginIssuer interface {
	CreateBrowserLoginURL() (string, error)
}

// CoreHandler maps the CoreService transport contract to app.Core.
type CoreHandler struct {
	core            *app.Core
	loginIssuer     BrowserLoginIssuer
	requestShutdown func()
}

// NewCoreHandler creates a CoreService handler with injected browser login and
// process-shutdown callbacks.
func NewCoreHandler(core *app.Core, loginIssuer BrowserLoginIssuer, requestShutdown func()) *CoreHandler {
	return &CoreHandler{
		core:            core,
		loginIssuer:     loginIssuer,
		requestShutdown: requestShutdown,
	}
}

// GetStatus returns the current immutable and mutable core status fields.
func (h *CoreHandler) GetStatus(_ context.Context, _ *connect.Request[connecttoolsv1.GetStatusRequest]) (*connect.Response[connecttoolsv1.CoreStatus], error) {
	status, err := coreStatusToProto(h.core.Status())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(status), nil
}

func coreStatusToProto(value app.Status) (*connecttoolsv1.CoreStatus, error) {
	status := value
	startedAt := timestamppb.New(status.StartedAt)
	if err := startedAt.CheckValid(); err != nil {
		return nil, errors.New("core start time is invalid")
	}
	return &connecttoolsv1.CoreStatus{
		Version:       status.Version,
		InstanceId:    status.InstanceID,
		StartedAt:     startedAt,
		ListenAddress: status.Address,
		McpEnabled:    status.MCPEnabled,
	}, nil
}

// CreateBrowserLogin creates a short-lived, single-use browser login URL.
func (h *CoreHandler) CreateBrowserLogin(_ context.Context, _ *connect.Request[connecttoolsv1.CreateBrowserLoginRequest]) (*connect.Response[connecttoolsv1.CreateBrowserLoginResponse], error) {
	loginURL, err := h.loginIssuer.CreateBrowserLoginURL()
	if err != nil {
		if errors.Is(err, ErrBrowserLoginCapacity) {
			return nil, connect.NewError(connect.CodeResourceExhausted, errors.New("too many pending browser logins"))
		}
		return nil, connect.NewError(connect.CodeInternal, errors.New("create browser login"))
	}
	return connect.NewResponse(&connecttoolsv1.CreateBrowserLoginResponse{Url: loginURL}), nil
}

// Shutdown requests graceful process shutdown and reports whether this request
// initiated the transition.
func (h *CoreHandler) Shutdown(_ context.Context, _ *connect.Request[connecttoolsv1.ShutdownRequest]) (*connect.Response[connecttoolsv1.ShutdownResponse], error) {
	accepted := h.core.Shutdown()
	if accepted && h.requestShutdown != nil {
		h.requestShutdown()
	}
	return connect.NewResponse(&connecttoolsv1.ShutdownResponse{Accepted: accepted}), nil
}

// SetMCPEnabled changes MCP availability for the running local instance.
func (h *CoreHandler) SetMCPEnabled(_ context.Context, request *connect.Request[connecttoolsv1.SetMCPEnabledRequest]) (*connect.Response[connecttoolsv1.CoreStatus], error) {
	if request == nil || request.Msg == nil {
		return nil, invalidArgument("request is required")
	}
	if err := h.core.SetMCPEnabled(request.Msg.Enabled); err != nil {
		return nil, connect.NewError(connect.CodeInternal, errors.New("persist MCP setting"))
	}
	status, err := coreStatusToProto(h.core.Status())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(status), nil
}

// WatchEvents streams live core events whose sequence is greater than the
// requested cursor.
func (h *CoreHandler) WatchEvents(ctx context.Context, request *connect.Request[connecttoolsv1.WatchEventsRequest], stream *connect.ServerStream[connecttoolsv1.CoreEvent]) error {
	events, err := h.core.Events(ctx, request.Msg.AfterSequence)
	if err != nil {
		if errors.Is(err, app.ErrTooManySubscribers) {
			return connect.NewError(connect.CodeResourceExhausted, errors.New("too many event subscribers"))
		}
		return connect.NewError(connect.CodeInternal, errors.New("subscribe to core events"))
	}

	for event := range events {
		mapped, err := coreEventToProto(event)
		if err != nil {
			return connect.NewError(connect.CodeInternal, err)
		}
		if err := stream.Send(mapped); err != nil {
			return err
		}
	}
	return nil
}

func coreEventToProto(event app.Event) (*connecttoolsv1.CoreEvent, error) {
	timestamp, err := toProtoTimestamp(event.Timestamp)
	if err != nil {
		return nil, errors.New("event timestamp is invalid")
	}
	result := &connecttoolsv1.CoreEvent{
		Sequence: event.Sequence, Timestamp: timestamp, Type: event.Type,
		Payload: append([]byte(nil), event.Payload...), PayloadTruncated: event.PayloadTruncated,
	}
	switch {
	case event.Profile != nil:
		mapped := &connecttoolsv1.ProfileEvent{ProfileId: event.Profile.ProfileID}
		switch event.Profile.Type {
		case app.ProfileEventCreated:
			mapped.Type = connecttoolsv1.ProfileEventType_PROFILE_EVENT_TYPE_CREATED
		case app.ProfileEventUpdated:
			mapped.Type = connecttoolsv1.ProfileEventType_PROFILE_EVENT_TYPE_UPDATED
		case app.ProfileEventDeleted:
			mapped.Type = connecttoolsv1.ProfileEventType_PROFILE_EVENT_TYPE_DELETED
		default:
			return nil, errors.New("profile event type is invalid")
		}
		result.EventBody = &connecttoolsv1.CoreEvent_ProfileEvent{ProfileEvent: mapped}
	case event.Session != nil:
		mappedSession, err := sessionToProto(event.Session.Session)
		if err != nil {
			return nil, err
		}
		mapped := &connecttoolsv1.SessionEvent{Session: mappedSession, SessionId: event.Session.SessionID}
		switch event.Session.Type {
		case session.EventCreated:
			mapped.Type = connecttoolsv1.SessionEventType_SESSION_EVENT_TYPE_CREATED
		case session.EventUpdated:
			mapped.Type = connecttoolsv1.SessionEventType_SESSION_EVENT_TYPE_UPDATED
		case session.EventRemoved:
			mapped.Type = connecttoolsv1.SessionEventType_SESSION_EVENT_TYPE_REMOVED
		default:
			return nil, errors.New("session event type is invalid")
		}
		result.EventBody = &connecttoolsv1.CoreEvent_SessionEvent{SessionEvent: mapped}
	case event.Record != nil:
		mapped := &connecttoolsv1.RecordEvent{
			SessionIds: append([]string(nil), event.Record.SessionIDs...), AffectedRecords: event.Record.AffectedRecords,
			AffectedPayloadBytes: uint64(event.Record.AffectedPayloadBytes),
		}
		if event.Record.Record != nil {
			mapped.Record, err = recordToProto(*event.Record.Record)
			if err != nil {
				return nil, err
			}
		}
		switch event.Record.Type {
		case records.EventAppended:
			mapped.Type = connecttoolsv1.RecordEventType_RECORD_EVENT_TYPE_APPENDED
		case records.EventCleared:
			mapped.Type = connecttoolsv1.RecordEventType_RECORD_EVENT_TYPE_CLEARED
		case records.EventRetentionLimitReached:
			mapped.Type = connecttoolsv1.RecordEventType_RECORD_EVENT_TYPE_RETENTION_LIMIT_REACHED
		default:
			return nil, errors.New("record event type is invalid")
		}
		result.EventBody = &connecttoolsv1.CoreEvent_RecordEvent{RecordEvent: mapped}
	case event.Lifecycle != nil:
		status, err := coreStatusToProto(event.Lifecycle.Status)
		if err != nil {
			return nil, err
		}
		mapped := &connecttoolsv1.CoreLifecycleEvent{Status: status}
		if event.Lifecycle.Type != app.EventCoreStopping {
			return nil, errors.New("core lifecycle event type is invalid")
		}
		mapped.Type = connecttoolsv1.CoreLifecycleEventType_CORE_LIFECYCLE_EVENT_TYPE_SHUTTING_DOWN
		result.EventBody = &connecttoolsv1.CoreEvent_CoreLifecycleEvent{CoreLifecycleEvent: mapped}
	}
	return result, nil
}
