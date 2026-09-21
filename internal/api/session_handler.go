package api

import (
	"context"

	connecttoolsv1 "connect-tools/gen/go/connecttools/v1"
	"connect-tools/internal/app"
	"connect-tools/internal/session"
	"connectrpc.com/connect"
)

// SessionHandler maps SessionService RPCs to runtime session application services.
type SessionHandler struct {
	sessions *app.SessionService
	tcp      *app.TCPService
	mqtt     *app.MQTTService
}

// NewSessionHandler creates a SessionService handler.
func NewSessionHandler(sessions *app.SessionService, tcpService *app.TCPService) *SessionHandler {
	return &SessionHandler{sessions: sessions, tcp: tcpService}
}

// NewSessionHandlerWithMQTT creates a handler that routes starts and stops to both transports.
func NewSessionHandlerWithMQTT(sessions *app.SessionService, tcpService *app.TCPService, mqttService *app.MQTTService) *SessionHandler {
	return &SessionHandler{sessions: sessions, tcp: tcpService, mqtt: mqttService}
}

// ListSessions returns one deterministic filtered runtime-session page.
func (h *SessionHandler) ListSessions(ctx context.Context, request *connect.Request[connecttoolsv1.ListSessionsRequest]) (*connect.Response[connecttoolsv1.ListSessionsResponse], error) {
	if h == nil || h.sessions == nil || request == nil || request.Msg == nil {
		return nil, invalidArgument("request and session service are required")
	}
	kinds := make(map[session.Kind]struct{})
	states := make(map[session.State]struct{})
	profileID, parentID := "", ""
	if request.Msg.Filter != nil {
		profileID, parentID = request.Msg.Filter.ProfileId, request.Msg.Filter.ParentId
		for _, value := range request.Msg.Filter.Kinds {
			mapped, err := sessionKindFromProto(value)
			if err != nil {
				return nil, invalidArgument(err.Error())
			}
			kinds[mapped] = struct{}{}
		}
		for _, value := range request.Msg.Filter.States {
			mapped, err := sessionStateFromProto(value)
			if err != nil {
				return nil, invalidArgument(err.Error())
			}
			states[mapped] = struct{}{}
		}
		if profileID != "" {
			if _, err := requireID(profileID, "profile ID"); err != nil {
				return nil, invalidArgument(err.Error())
			}
		}
		if parentID != "" {
			if _, err := requireID(parentID, "parent ID"); err != nil {
				return nil, invalidArgument(err.Error())
			}
		}
	}
	values, err := h.sessions.List(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	filtered := make([]session.Session, 0, len(values))
	for _, value := range values {
		if len(kinds) > 0 {
			if _, ok := kinds[value.Kind]; !ok {
				continue
			}
		}
		if len(states) > 0 {
			if _, ok := states[value.State]; !ok {
				continue
			}
		}
		if profileID != "" && value.ProfileID != profileID {
			continue
		}
		if parentID != "" && value.ParentID != parentID {
			continue
		}
		filtered = append(filtered, value)
	}
	start, end, next, err := pageBounds(len(filtered), request.Msg.PageSize, request.Msg.PageToken)
	if err != nil {
		return nil, invalidArgument(err.Error())
	}
	response := &connecttoolsv1.ListSessionsResponse{Sessions: make([]*connecttoolsv1.Session, 0, end-start), NextPageToken: next}
	for _, value := range filtered[start:end] {
		mapped, err := sessionToProto(value)
		if err != nil {
			return nil, rpcError(err)
		}
		response.Sessions = append(response.Sessions, mapped)
	}
	return connect.NewResponse(response), nil
}

// GetSession returns one runtime session snapshot.
func (h *SessionHandler) GetSession(ctx context.Context, request *connect.Request[connecttoolsv1.GetSessionRequest]) (*connect.Response[connecttoolsv1.GetSessionResponse], error) {
	if h == nil || h.sessions == nil || request == nil || request.Msg == nil {
		return nil, invalidArgument("request and session service are required")
	}
	id, err := requireID(request.Msg.SessionId, "session ID")
	if err != nil {
		return nil, invalidArgument(err.Error())
	}
	value, err := h.sessions.Get(ctx, id)
	if err != nil {
		return nil, rpcError(err)
	}
	mapped, err := sessionToProto(value)
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&connecttoolsv1.GetSessionResponse{Session: mapped}), nil
}

// StartSession starts the real TCP transport represented by a saved profile.
func (h *SessionHandler) StartSession(ctx context.Context, request *connect.Request[connecttoolsv1.StartSessionRequest]) (*connect.Response[connecttoolsv1.StartSessionResponse], error) {
	if h == nil || h.tcp == nil || request == nil || request.Msg == nil {
		return nil, invalidArgument("request and TCP service are required")
	}
	profileID, err := requireID(request.Msg.ProfileId, "profile ID")
	if err != nil {
		return nil, invalidArgument(err.Error())
	}
	profileValue, err := h.sessions.GetProfile(ctx, profileID)
	if err != nil {
		return nil, rpcError(err)
	}
	var value session.Session
	if profileValue.Type == "mqtt-client" {
		if h.mqtt == nil {
			return nil, connect.NewError(connect.CodeFailedPrecondition, app.ErrProfileNotMQTT)
		}
		value, err = h.mqtt.Start(ctx, profileID, request.Msg.GetDisplayName())
	} else {
		value, err = h.tcp.Start(ctx, profileID, request.Msg.GetDisplayName())
	}
	if err != nil {
		return nil, rpcError(err)
	}
	mapped, err := sessionToProto(value)
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&connecttoolsv1.StartSessionResponse{Session: mapped}), nil
}

// StopSession stops a TCP session and waits for its owned runtime resources.
func (h *SessionHandler) StopSession(ctx context.Context, request *connect.Request[connecttoolsv1.StopSessionRequest]) (*connect.Response[connecttoolsv1.StopSessionResponse], error) {
	if h == nil || h.tcp == nil || request == nil || request.Msg == nil {
		return nil, invalidArgument("request and TCP service are required")
	}
	id, err := requireID(request.Msg.SessionId, "session ID")
	if err != nil {
		return nil, invalidArgument(err.Error())
	}
	current, err := h.sessions.Get(ctx, id)
	if err != nil {
		return nil, rpcError(err)
	}
	var value session.Session
	if current.Kind == session.KindMQTTClient {
		if h.mqtt == nil {
			return nil, connect.NewError(connect.CodeFailedPrecondition, app.ErrProfileNotMQTT)
		}
		value, err = h.mqtt.Stop(ctx, id)
	} else {
		value, err = h.tcp.Stop(ctx, id)
	}
	if err != nil {
		return nil, rpcError(err)
	}
	mapped, err := sessionToProto(value)
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&connecttoolsv1.StopSessionResponse{Session: mapped}), nil
}
