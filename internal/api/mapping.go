package api

import (
	"errors"
	"strings"
	"time"

	connecttoolsv1 "connect-tools/gen/go/connecttools/v1"
	"connect-tools/internal/convert"
	"connect-tools/internal/profile"
	"connect-tools/internal/records"
	"connect-tools/internal/session"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func requireID(value, field string) (string, error) {
	if value == "" || strings.TrimSpace(value) != value {
		return "", errors.New(field + " is required")
	}
	return value, nil
}

func toProtoTimestamp(value time.Time) (*timestamppb.Timestamp, error) {
	timestamp := timestamppb.New(value)
	if err := timestamp.CheckValid(); err != nil {
		return nil, errors.New("timestamp is invalid")
	}
	return timestamp, nil
}

func validateProtoTimestamp(value *timestamppb.Timestamp) error {
	if value == nil {
		return nil
	}
	if err := value.CheckValid(); err != nil {
		return errors.New("timestamp is invalid")
	}
	return nil
}

func profileFromProto(value *connecttoolsv1.Profile, update bool) (profile.Profile, error) {
	if value == nil {
		return profile.Profile{}, errors.New("profile is required")
	}
	if err := validateProtoTimestamp(value.CreatedAt); err != nil {
		return profile.Profile{}, err
	}
	if err := validateProtoTimestamp(value.UpdatedAt); err != nil {
		return profile.Profile{}, err
	}
	if update {
		if _, err := requireID(value.Id, "profile ID"); err != nil {
			return profile.Profile{}, err
		}
	} else if value.Id != "" || value.CreatedAt != nil || value.UpdatedAt != nil {
		return profile.Profile{}, errors.New("server-managed profile fields must be omitted")
	}

	result := profile.Profile{ID: value.Id, Name: value.Name, Description: value.Description}
	switch connection := value.Connection.(type) {
	case *connecttoolsv1.Profile_TcpClient:
		if connection.TcpClient == nil {
			return profile.Profile{}, errors.New("TCP client profile is required")
		}
		framing, err := framingFromProto(connection.TcpClient.Framing)
		if err != nil {
			return profile.Profile{}, err
		}
		result.Type = profile.TypeTCPClient
		result.TCPClient = &profile.TCPClient{
			Host: connection.TcpClient.Host, Port: int(connection.TcpClient.Port), Framing: framing,
			AutoReconnect: connection.TcpClient.AutoReconnect, ConnectTimeoutMS: int64(connection.TcpClient.ConnectTimeoutMs),
			ReadTimeoutMS: int64(connection.TcpClient.ReadTimeoutMs), WriteTimeoutMS: int64(connection.TcpClient.WriteTimeoutMs),
			KeepAlive: connection.TcpClient.KeepAlive, KeepAlivePeriodMS: int64(connection.TcpClient.KeepAlivePeriodMs),
			NoDelay: connection.TcpClient.NoDelay,
		}
	case *connecttoolsv1.Profile_TcpServer:
		if connection.TcpServer == nil {
			return profile.Profile{}, errors.New("TCP server profile is required")
		}
		framing, err := framingFromProto(connection.TcpServer.Framing)
		if err != nil {
			return profile.Profile{}, err
		}
		result.Type = profile.TypeTCPServer
		result.TCPServer = &profile.TCPServer{
			BindHost: connection.TcpServer.BindHost, Port: int(connection.TcpServer.Port),
			MaxConnections: int(connection.TcpServer.MaxConnections), Framing: framing,
			KeepAlive: connection.TcpServer.KeepAlive, NoDelay: connection.TcpServer.NoDelay,
		}
	case *connecttoolsv1.Profile_Mqtt:
		mqtt, err := mqttProfileFromProto(connection.Mqtt)
		if err != nil {
			return profile.Profile{}, err
		}
		result.Type = profile.TypeMQTTClient
		result.MQTT = mqtt
	default:
		return profile.Profile{}, errors.New("profile connection is required")
	}
	return result, nil
}

func profileToProto(value profile.Profile) (*connecttoolsv1.Profile, error) {
	createdAt, err := toProtoTimestamp(value.CreatedAt)
	if err != nil {
		return nil, err
	}
	updatedAt, err := toProtoTimestamp(value.UpdatedAt)
	if err != nil {
		return nil, err
	}
	result := &connecttoolsv1.Profile{Id: value.ID, Name: value.Name, Description: value.Description, CreatedAt: createdAt, UpdatedAt: updatedAt}
	switch value.Type {
	case profile.TypeTCPClient:
		if value.TCPClient == nil {
			return nil, errors.New("TCP client profile is invalid")
		}
		result.Connection = &connecttoolsv1.Profile_TcpClient{TcpClient: &connecttoolsv1.TCPClientProfile{
			Host: value.TCPClient.Host, Port: uint32(value.TCPClient.Port), Framing: framingToProto(value.TCPClient.Framing),
			AutoReconnect: value.TCPClient.AutoReconnect, ConnectTimeoutMs: uint32(value.TCPClient.ConnectTimeoutMS),
			ReadTimeoutMs: uint32(value.TCPClient.ReadTimeoutMS), WriteTimeoutMs: uint32(value.TCPClient.WriteTimeoutMS),
			KeepAlive: value.TCPClient.KeepAlive, KeepAlivePeriodMs: uint32(value.TCPClient.KeepAlivePeriodMS), NoDelay: value.TCPClient.NoDelay,
		}}
	case profile.TypeTCPServer:
		if value.TCPServer == nil {
			return nil, errors.New("TCP server profile is invalid")
		}
		result.Connection = &connecttoolsv1.Profile_TcpServer{TcpServer: &connecttoolsv1.TCPServerProfile{
			BindHost: value.TCPServer.BindHost, Port: uint32(value.TCPServer.Port), MaxConnections: uint32(value.TCPServer.MaxConnections),
			Framing: framingToProto(value.TCPServer.Framing), KeepAlive: value.TCPServer.KeepAlive, NoDelay: value.TCPServer.NoDelay,
		}}
	case profile.TypeMQTTClient:
		if value.MQTT == nil {
			return nil, errors.New("MQTT profile is invalid")
		}
		result.Connection = &connecttoolsv1.Profile_Mqtt{Mqtt: mqttProfileToProto(*value.MQTT)}
	default:
		return nil, errors.New("profile type is invalid")
	}
	return result, nil
}

func framingFromProto(value *connecttoolsv1.FramingConfig) (profile.FramingConfig, error) {
	if value == nil {
		return profile.FramingConfig{}, errors.New("framing configuration is required")
	}
	result := profile.FramingConfig{FixedLength: int(value.FixedLength), Delimiter: append([]byte(nil), value.Delimiter...), IncludeDelimiter: value.IncludeDelimiter}
	switch value.Mode {
	case connecttoolsv1.FrameMode_FRAME_MODE_RAW_READ:
		result.Mode = profile.FrameModeRawRead
	case connecttoolsv1.FrameMode_FRAME_MODE_FIXED_LENGTH:
		result.Mode = profile.FrameModeFixedLength
	case connecttoolsv1.FrameMode_FRAME_MODE_DELIMITER:
		result.Mode = profile.FrameModeDelimiter
	default:
		return profile.FramingConfig{}, errors.New("frame mode is required")
	}
	return result, nil
}

func framingToProto(value profile.FramingConfig) *connecttoolsv1.FramingConfig {
	result := &connecttoolsv1.FramingConfig{FixedLength: uint32(value.FixedLength), Delimiter: append([]byte(nil), value.Delimiter...), IncludeDelimiter: value.IncludeDelimiter}
	switch value.Mode {
	case profile.FrameModeRawRead:
		result.Mode = connecttoolsv1.FrameMode_FRAME_MODE_RAW_READ
	case profile.FrameModeFixedLength:
		result.Mode = connecttoolsv1.FrameMode_FRAME_MODE_FIXED_LENGTH
	case profile.FrameModeDelimiter:
		result.Mode = connecttoolsv1.FrameMode_FRAME_MODE_DELIMITER
	}
	return result
}

func sessionToProto(value session.Session) (*connecttoolsv1.Session, error) {
	startedAt, err := toProtoTimestamp(value.CreatedAt)
	if err != nil {
		return nil, err
	}
	updatedAt, err := toProtoTimestamp(value.UpdatedAt)
	if err != nil {
		return nil, err
	}
	result := &connecttoolsv1.Session{
		Id: value.ID, ProfileId: value.ProfileID, ParentId: value.ParentID, Kind: sessionKindToProto(value.Kind), State: sessionStateToProto(value.State),
		DisplayName: value.DisplayName, LocalAddress: value.LocalAddress, RemoteAddress: value.RemoteAddress, StartedAt: startedAt, UpdatedAt: updatedAt,
	}
	if value.LastError != nil {
		timestamp, err := toProtoTimestamp(value.LastError.Timestamp)
		if err != nil {
			return nil, err
		}
		result.LastError = &connecttoolsv1.SessionError{
			Code: value.LastError.Code, Message: value.LastError.Message, Operation: value.LastError.Operation,
			Retryable: value.LastError.Retryable, Timestamp: timestamp, Details: cloneMap(value.LastError.Details),
		}
	}
	if result.Kind == connecttoolsv1.SessionKind_SESSION_KIND_UNSPECIFIED || result.State == connecttoolsv1.SessionState_SESSION_STATE_UNSPECIFIED {
		return nil, errors.New("session contains an invalid enum")
	}
	return result, nil
}

func sessionKindToProto(value session.Kind) connecttoolsv1.SessionKind {
	switch value {
	case session.KindTCPClient:
		return connecttoolsv1.SessionKind_SESSION_KIND_TCP_CLIENT
	case session.KindTCPServer:
		return connecttoolsv1.SessionKind_SESSION_KIND_TCP_SERVER
	case session.KindTCPPeer:
		return connecttoolsv1.SessionKind_SESSION_KIND_TCP_PEER
	case session.KindMQTTClient:
		return connecttoolsv1.SessionKind_SESSION_KIND_MQTT_CLIENT
	default:
		return connecttoolsv1.SessionKind_SESSION_KIND_UNSPECIFIED
	}
}

func sessionKindFromProto(value connecttoolsv1.SessionKind) (session.Kind, error) {
	switch value {
	case connecttoolsv1.SessionKind_SESSION_KIND_TCP_CLIENT:
		return session.KindTCPClient, nil
	case connecttoolsv1.SessionKind_SESSION_KIND_TCP_SERVER:
		return session.KindTCPServer, nil
	case connecttoolsv1.SessionKind_SESSION_KIND_TCP_PEER:
		return session.KindTCPPeer, nil
	case connecttoolsv1.SessionKind_SESSION_KIND_MQTT_CLIENT:
		return session.KindMQTTClient, nil
	default:
		return "", errors.New("session kind is invalid")
	}
}

func sessionStateToProto(value session.State) connecttoolsv1.SessionState {
	switch value {
	case session.StateStopped:
		return connecttoolsv1.SessionState_SESSION_STATE_STOPPED
	case session.StateStarting:
		return connecttoolsv1.SessionState_SESSION_STATE_STARTING
	case session.StateRunning:
		return connecttoolsv1.SessionState_SESSION_STATE_RUNNING
	case session.StateStopping:
		return connecttoolsv1.SessionState_SESSION_STATE_STOPPING
	case session.StateError:
		return connecttoolsv1.SessionState_SESSION_STATE_ERROR
	default:
		return connecttoolsv1.SessionState_SESSION_STATE_UNSPECIFIED
	}
}

func sessionStateFromProto(value connecttoolsv1.SessionState) (session.State, error) {
	switch value {
	case connecttoolsv1.SessionState_SESSION_STATE_STOPPED:
		return session.StateStopped, nil
	case connecttoolsv1.SessionState_SESSION_STATE_STARTING:
		return session.StateStarting, nil
	case connecttoolsv1.SessionState_SESSION_STATE_RUNNING:
		return session.StateRunning, nil
	case connecttoolsv1.SessionState_SESSION_STATE_STOPPING:
		return session.StateStopping, nil
	case connecttoolsv1.SessionState_SESSION_STATE_ERROR:
		return session.StateError, nil
	default:
		return "", errors.New("session state is invalid")
	}
}

func recordFilterFromProto(value *connecttoolsv1.RecordFilter) (records.Filter, error) {
	if value == nil {
		return records.Filter{}, nil
	}
	result := records.Filter{SessionIDs: append([]string(nil), value.SessionIds...), Contains: append([]byte(nil), value.PayloadContains...)}
	for _, id := range result.SessionIDs {
		if _, err := requireID(id, "session ID"); err != nil {
			return records.Filter{}, err
		}
	}
	if value.Direction != nil {
		direction, err := directionFromProto(*value.Direction)
		if err != nil {
			return records.Filter{}, err
		}
		result.Direction = &direction
	}
	if value.Actor != nil {
		actor, err := actorFromProto(*value.Actor)
		if err != nil {
			return records.Filter{}, err
		}
		result.Actor = &actor
	}
	return result, nil
}

func recordToProto(value records.Record) (*connecttoolsv1.DebugRecord, error) {
	timestamp, err := toProtoTimestamp(value.Timestamp)
	if err != nil {
		return nil, err
	}
	result := &connecttoolsv1.DebugRecord{
		Sequence: value.Sequence, Timestamp: timestamp, SessionId: value.SessionID, PeerId: value.PeerID,
		Direction: directionToProto(value.Direction), Actor: actorToProto(value.Actor), Payload: append([]byte(nil), value.Payload...),
		ProtocolMetadata: cloneMap(value.ProtocolMetadata),
	}
	if result.Direction == connecttoolsv1.Direction_DIRECTION_UNSPECIFIED || result.Actor == connecttoolsv1.Actor_ACTOR_UNSPECIFIED {
		return nil, errors.New("record contains an invalid enum")
	}
	if value.Error != nil {
		errorTime, err := toProtoTimestamp(value.Error.Timestamp)
		if err != nil {
			return nil, err
		}
		result.Error = &connecttoolsv1.RecordError{
			Code: value.Error.Code, Message: value.Error.Message, Operation: value.Error.Operation,
			Retryable: value.Error.Retryable, Timestamp: errorTime, Details: cloneMap(value.Error.Details),
		}
	}
	return result, nil
}

func directionFromProto(value connecttoolsv1.Direction) (records.Direction, error) {
	switch value {
	case connecttoolsv1.Direction_DIRECTION_INBOUND:
		return records.DirectionInbound, nil
	case connecttoolsv1.Direction_DIRECTION_OUTBOUND:
		return records.DirectionOutbound, nil
	case connecttoolsv1.Direction_DIRECTION_INTERNAL:
		return records.DirectionInternal, nil
	default:
		return 0, errors.New("direction is invalid")
	}
}

func directionToProto(value records.Direction) connecttoolsv1.Direction {
	switch value {
	case records.DirectionInbound:
		return connecttoolsv1.Direction_DIRECTION_INBOUND
	case records.DirectionOutbound:
		return connecttoolsv1.Direction_DIRECTION_OUTBOUND
	case records.DirectionInternal:
		return connecttoolsv1.Direction_DIRECTION_INTERNAL
	default:
		return connecttoolsv1.Direction_DIRECTION_UNSPECIFIED
	}
}

func actorFromProto(value connecttoolsv1.Actor) (records.Actor, error) {
	switch value {
	case connecttoolsv1.Actor_ACTOR_REMOTE:
		return records.ActorRemote, nil
	case connecttoolsv1.Actor_ACTOR_USER:
		return records.ActorUser, nil
	case connecttoolsv1.Actor_ACTOR_CLI:
		return records.ActorCLI, nil
	case connecttoolsv1.Actor_ACTOR_MCP:
		return records.ActorMCP, nil
	case connecttoolsv1.Actor_ACTOR_SYSTEM:
		return records.ActorSystem, nil
	default:
		return 0, errors.New("actor is invalid")
	}
}

func actorToProto(value records.Actor) connecttoolsv1.Actor {
	switch value {
	case records.ActorRemote:
		return connecttoolsv1.Actor_ACTOR_REMOTE
	case records.ActorUser:
		return connecttoolsv1.Actor_ACTOR_USER
	case records.ActorCLI:
		return connecttoolsv1.Actor_ACTOR_CLI
	case records.ActorMCP:
		return connecttoolsv1.Actor_ACTOR_MCP
	case records.ActorSystem:
		return connecttoolsv1.Actor_ACTOR_SYSTEM
	default:
		return connecttoolsv1.Actor_ACTOR_UNSPECIFIED
	}
}

func payloadFromProto(value *connecttoolsv1.PayloadInput) (convert.PayloadInput, error) {
	if value == nil {
		return convert.PayloadInput{}, errors.New("payload is required")
	}
	result := convert.PayloadInput{Data: value.Data, Charset: convert.Charset(value.Charset), AppendDelimiter: append([]byte(nil), value.AppendDelimiter...)}
	switch value.Encoding {
	case connecttoolsv1.PayloadEncoding_PAYLOAD_ENCODING_TEXT:
		result.Encoding = convert.EncodingText
	case connecttoolsv1.PayloadEncoding_PAYLOAD_ENCODING_HEX:
		result.Encoding = convert.EncodingHex
	case connecttoolsv1.PayloadEncoding_PAYLOAD_ENCODING_BASE64:
		result.Encoding = convert.EncodingBase64
	default:
		return convert.PayloadInput{}, errors.New("payload encoding is required")
	}
	return result, nil
}

func cloneMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
