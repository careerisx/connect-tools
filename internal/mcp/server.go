package mcp

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"connect-tools/internal/app"
	ctconvert "connect-tools/internal/convert"
	ctmqtt "connect-tools/internal/mqtt"
	"connect-tools/internal/profile"
	"connect-tools/internal/records"
	"connect-tools/internal/secrets"
	"connect-tools/internal/session"
)

const maxBodyBytes = 24 << 20

type Dependencies struct {
	Core     *app.Core
	Profiles *app.ProfileService
	Sessions *app.SessionService
	TCP      *app.TCPService
	MQTT     *app.MQTTService
	Records  *records.Store
	Secrets  secrets.Store
}

type Handler struct {
	token string
	deps  Dependencies
}

func NewHandler(token string, deps Dependencies) (*Handler, error) {
	if token == "" || deps.Core == nil || deps.Profiles == nil || deps.Sessions == nil || deps.TCP == nil || deps.MQTT == nil || deps.Records == nil || deps.Secrets == nil {
		return nil, errors.New("MCP dependencies are required")
	}
	return &Handler{token: token, deps: deps}, nil
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}
type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}
type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}
type toolCall struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Cache-Control", "no-store")
	if request.Method != http.MethodPost {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !h.deps.Core.Status().MCPEnabled {
		http.Error(writer, "MCP is disabled", http.StatusNotFound)
		return
	}
	if !validBearer(request.Header.Get("Authorization"), h.token) {
		writer.Header().Set("WWW-Authenticate", "Bearer")
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	if !validHost(request.Host) || !validOrigin(request.Header.Get("Origin")) {
		http.Error(writer, "forbidden", http.StatusForbidden)
		return
	}
	request.Body = http.MaxBytesReader(writer, request.Body, maxBodyBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var rpc rpcRequest
	if err := decoder.Decode(&rpc); err != nil {
		writeRPC(writer, rpcResponse{JSONRPC: "2.0", ID: rpc.ID, Error: &rpcError{Code: -32700, Message: "invalid JSON-RPC request"}})
		return
	}
	if rpc.JSONRPC != "2.0" {
		writeRPC(writer, rpcResponse{JSONRPC: "2.0", ID: rpc.ID, Error: &rpcError{Code: -32600, Message: "JSON-RPC 2.0 is required"}})
		return
	}
	if len(rpc.ID) == 0 {
		writer.WriteHeader(http.StatusAccepted)
		return
	}
	response := rpcResponse{JSONRPC: "2.0", ID: rpc.ID}
	switch rpc.Method {
	case "initialize":
		response.Result = map[string]any{"protocolVersion": "2025-11-25", "capabilities": map[string]any{"tools": map[string]any{"listChanged": false}}, "serverInfo": map[string]any{"name": "connect-tools", "version": h.deps.Core.Status().Version}, "instructions": "Inspect and debug local TCP and MQTT sessions. Read records before sending data; all network mutations are explicit tools."}
	case "ping":
		response.Result = map[string]any{}
	case "tools/list":
		response.Result = map[string]any{"tools": toolDefinitions()}
	case "tools/call":
		var call toolCall
		if err := json.Unmarshal(rpc.Params, &call); err != nil {
			response.Error = &rpcError{Code: -32602, Message: "invalid tool call"}
			break
		}
		result, err := h.callTool(request.Context(), call)
		if err != nil {
			response.Result = map[string]any{"content": []any{map[string]any{"type": "text", "text": err.Error()}}, "isError": true}
		} else {
			response.Result = map[string]any{"content": []any{map[string]any{"type": "text", "text": mustJSON(result)}}, "structuredContent": result, "isError": false}
		}
	default:
		response.Error = &rpcError{Code: -32601, Message: "method not found"}
	}
	writeRPC(writer, response)
}

func (h *Handler) callTool(ctx context.Context, call toolCall) (any, error) {
	args := map[string]any{}
	if len(call.Arguments) > 0 {
		if err := json.Unmarshal(call.Arguments, &args); err != nil {
			return nil, errors.New("arguments must be an object")
		}
	}
	switch call.Name {
	case "list_profiles":
		values, err := h.deps.Profiles.List(ctx)
		if err != nil {
			return nil, err
		}
		result := make([]any, 0, len(values))
		for _, value := range values {
			result = append(result, publicProfile(value))
		}
		return map[string]any{"profiles": result}, nil
	case "list_sessions":
		values, err := h.deps.Sessions.List(ctx)
		return map[string]any{"sessions": values}, err
	case "get_session":
		return h.deps.Sessions.Get(ctx, stringArg(args, "session_id"))
	case "read_records":
		return h.readRecords(args)
	case "wait_for_record":
		return h.waitRecord(ctx, args)
	case "get_diagnostics":
		return h.diagnostics(ctx)
	case "convert_payload":
		return convertTool(args)
	case "start_profile":
		return h.startProfile(ctx, stringArg(args, "profile_id"))
	case "create_tcp_client":
		return h.createTCP(ctx, args, false)
	case "create_tcp_server":
		return h.createTCP(ctx, args, true)
	case "create_mqtt_client":
		return h.createMQTT(ctx, args)
	case "stop_session":
		return h.stopSession(ctx, stringArg(args, "session_id"))
	case "send_tcp":
		payload, err := decodeToolPayload(args)
		if err != nil {
			return nil, err
		}
		result, err := h.deps.TCP.Send(ctx, stringArg(args, "session_id"), payload, records.ActorMCP)
		return result, err
	case "publish_mqtt":
		return h.publishMQTT(ctx, args)
	case "subscribe_mqtt":
		return h.subscribeMQTT(ctx, args)
	case "unsubscribe_mqtt":
		return h.unsubscribeMQTT(ctx, args)
	default:
		return nil, fmt.Errorf("unknown tool %q", call.Name)
	}
}

func (h *Handler) readRecords(args map[string]any) (any, error) {
	limit := intArg(args, "limit", 50)
	if limit < 1 || limit > 500 {
		return nil, errors.New("limit must be between 1 and 500")
	}
	filter := records.Filter{After: uint64Arg(args, "after"), Limit: limit}
	if id := stringArg(args, "session_id"); id != "" {
		filter.SessionIDs = []string{id}
	}
	page, err := h.deps.Records.List(filter)
	if err != nil {
		return nil, err
	}
	return map[string]any{"records": recordOutputs(page.Records), "next_cursor": page.NextCursor, "has_more": page.HasMore}, nil
}

func (h *Handler) waitRecord(ctx context.Context, args map[string]any) (any, error) {
	timeout := intArg(args, "timeout_seconds", 30)
	if timeout < 1 || timeout > 120 {
		return nil, errors.New("timeout_seconds must be between 1 and 120")
	}
	waitCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()
	match := records.Match{After: uint64Arg(args, "after")}
	if id := stringArg(args, "session_id"); id != "" {
		match.SessionIDs = []string{id}
	}
	value, err := h.deps.Records.Wait(waitCtx, match)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, errors.New("WAIT_TIMEOUT")
		}
		return nil, err
	}
	return recordOutput(value), nil
}

func (h *Handler) diagnostics(ctx context.Context) (any, error) {
	sessions, err := h.deps.Sessions.List(ctx)
	if err != nil {
		return nil, err
	}
	metrics := h.deps.Records.Metrics()
	status := h.deps.Core.Status()
	return map[string]any{"version": status.Version, "instance_id": status.InstanceID, "started_at": status.StartedAt, "mcp_enabled": status.MCPEnabled, "session_count": len(sessions), "retained_records": metrics.Records, "retained_payload_bytes": metrics.PayloadBytes, "waiters": metrics.Waiters}, nil
}

func (h *Handler) startProfile(ctx context.Context, id string) (session.Session, error) {
	value, err := h.deps.Profiles.Get(ctx, id)
	if err != nil {
		return session.Session{}, err
	}
	if value.Type == profile.TypeMQTTClient {
		return h.deps.MQTT.Start(ctx, id, "")
	}
	return h.deps.TCP.Start(ctx, id, "")
}

func (h *Handler) stopSession(ctx context.Context, id string) (session.Session, error) {
	value, err := h.deps.Sessions.Get(ctx, id)
	if err != nil {
		return session.Session{}, err
	}
	if value.Kind == session.KindMQTTClient {
		return h.deps.MQTT.Stop(ctx, id)
	}
	return h.deps.TCP.Stop(ctx, id)
}

func (h *Handler) createTCP(ctx context.Context, args map[string]any, server bool) (session.Session, error) {
	port := intArg(args, "port", 0)
	if port < 1 || port > 65535 {
		return session.Session{}, errors.New("port must be between 1 and 65535")
	}
	host := stringArg(args, "host")
	if server {
		host = stringArg(args, "bind_host")
		if host == "" {
			host = "127.0.0.1"
		}
		if !isLoopback(host) {
			return session.Session{}, errors.New("MCP TCP servers must bind to loopback")
		}
	} else if host == "" {
		return session.Session{}, errors.New("host is required")
	}
	value := profile.Profile{Name: "MCP temporary TCP", Type: profile.TypeTCPClient, TCPClient: &profile.TCPClient{Host: host, Port: port, Framing: profile.FramingConfig{Mode: profile.FrameModeRawRead}, AutoReconnect: boolArg(args, "auto_reconnect")}}
	if server {
		value.Type = profile.TypeTCPServer
		value.TCPClient = nil
		value.TCPServer = &profile.TCPServer{BindHost: host, Port: port, MaxConnections: intArg(args, "max_connections", 128), Framing: profile.FramingConfig{Mode: profile.FrameModeRawRead}}
	}
	created, err := h.deps.Profiles.Create(ctx, value)
	if err != nil {
		return session.Session{}, err
	}
	started, startErr := h.deps.TCP.Start(ctx, created.ID, "MCP temporary TCP")
	_ = h.deps.Profiles.Delete(context.WithoutCancel(ctx), created.ID)
	return started, startErr
}

func (h *Handler) createMQTT(ctx context.Context, args map[string]any) (session.Session, error) {
	version := profile.MQTTVersion5
	if stringArg(args, "version") == "3.1.1" {
		version = profile.MQTTVersion311
	}
	host := stringArg(args, "host")
	port := intArg(args, "port", 1883)
	if host == "" || port < 1 || port > 65535 {
		return session.Session{}, errors.New("valid host and port are required")
	}
	value := profile.Profile{Name: "MCP temporary MQTT", Type: profile.TypeMQTTClient, MQTT: &profile.MQTTClient{Version: version, Host: host, Port: port, ClientID: stringArg(args, "client_id"), Username: stringArg(args, "username"), KeepAliveSeconds: 30, CleanStart: true, Reconnect: profile.ReconnectPolicy{Enabled: true}}}
	updates := app.ProfileSecretUpdates{}
	if ref := stringArg(args, "credential_ref"); ref != "" {
		secret, err := h.deps.Secrets.Get(ctx, ref)
		if err != nil {
			return session.Session{}, err
		}
		defer clear(secret)
		updates.Password = app.SecretUpdate{Action: app.SecretSet, Value: secret}
	}
	created, err := h.deps.Profiles.CreateWithSecrets(ctx, value, updates)
	if err != nil {
		return session.Session{}, err
	}
	started, startErr := h.deps.MQTT.Start(ctx, created.ID, "MCP temporary MQTT")
	_ = h.deps.Profiles.Delete(context.WithoutCancel(ctx), created.ID)
	return started, startErr
}

func (h *Handler) publishMQTT(ctx context.Context, args map[string]any) (ctmqtt.PublishAck, error) {
	payload, err := decodeToolPayload(args)
	if err != nil {
		return ctmqtt.PublishAck{}, err
	}
	qos := intArg(args, "qos", 0)
	if qos < 0 || qos > 2 {
		return ctmqtt.PublishAck{}, errors.New("qos must be 0, 1, or 2")
	}
	return h.deps.MQTT.Publish(ctx, stringArg(args, "session_id"), ctmqtt.Publish{Topic: stringArg(args, "topic"), Payload: payload, QoS: byte(intArg(args, "qos", 0)), Retain: boolArg(args, "retain")}, records.ActorMCP)
}
func (h *Handler) subscribeMQTT(ctx context.Context, args map[string]any) (ctmqtt.SubscribeAck, error) {
	qos := intArg(args, "qos", 0)
	if qos < 0 || qos > 2 {
		return ctmqtt.SubscribeAck{}, errors.New("qos must be 0, 1, or 2")
	}
	return h.deps.MQTT.Subscribe(ctx, stringArg(args, "session_id"), ctmqtt.SubscriptionBatch{Subscriptions: []ctmqtt.Subscription{{Filter: stringArg(args, "filter"), QoS: byte(intArg(args, "qos", 0))}}})
}
func (h *Handler) unsubscribeMQTT(ctx context.Context, args map[string]any) (ctmqtt.UnsubscribeAck, error) {
	return h.deps.MQTT.Unsubscribe(ctx, stringArg(args, "session_id"), []string{stringArg(args, "filter")})
}

func toolDefinitions() []map[string]any {
	read := true
	write := false
	definition := func(name, description string, properties map[string]any, required []string, readOnly bool) map[string]any {
		return map[string]any{"name": name, "description": description, "inputSchema": map[string]any{"type": "object", "properties": properties, "required": required, "additionalProperties": false}, "annotations": map[string]any{"readOnlyHint": readOnly}}
	}
	stringProp := func(description string) map[string]any {
		return map[string]any{"type": "string", "description": description}
	}
	integerProp := func(description string) map[string]any {
		return map[string]any{"type": "integer", "description": description}
	}
	return []map[string]any{
		definition("list_profiles", "List saved TCP and MQTT profiles without credentials.", map[string]any{}, nil, read), definition("list_sessions", "List runtime sessions.", map[string]any{}, nil, read), definition("get_session", "Get one runtime session.", map[string]any{"session_id": stringProp("runtime session ID")}, []string{"session_id"}, read),
		definition("read_records", "Read bounded debug records.", map[string]any{"session_id": stringProp("optional session ID"), "after": integerProp("exclusive cursor"), "limit": integerProp("1..500")}, nil, read), definition("wait_for_record", "Wait for a matching debug record.", map[string]any{"session_id": stringProp("optional session ID"), "after": integerProp("exclusive cursor"), "timeout_seconds": integerProp("1..120")}, nil, read), definition("get_diagnostics", "Get redacted runtime diagnostics.", map[string]any{}, nil, read), definition("convert_payload", "Convert text, HEX, or Base64 payloads.", payloadSchema(), []string{"data", "encoding"}, read),
		definition("start_profile", "Start a saved profile.", map[string]any{"profile_id": stringProp("saved profile ID")}, []string{"profile_id"}, write), definition("create_tcp_client", "Create and start a temporary TCP client.", map[string]any{"host": stringProp("remote host"), "port": integerProp("remote port"), "auto_reconnect": map[string]any{"type": "boolean"}}, []string{"host", "port"}, write), definition("create_tcp_server", "Create and start a temporary loopback TCP server.", map[string]any{"bind_host": stringProp("loopback bind host"), "port": integerProp("listen port"), "max_connections": integerProp("peer limit")}, []string{"port"}, write), definition("create_mqtt_client", "Create and start a temporary MQTT client using an optional credential reference.", map[string]any{"host": stringProp("broker host"), "port": integerProp("broker port"), "version": stringProp("3.1.1 or 5.0"), "client_id": stringProp("client ID"), "username": stringProp("username"), "credential_ref": stringProp("managed credential reference")}, []string{"host"}, write),
		definition("stop_session", "Stop a runtime session.", map[string]any{"session_id": stringProp("runtime session ID")}, []string{"session_id"}, write), definition("send_tcp", "Send bytes to a TCP client, peer, or server.", mergeMaps(payloadSchema(), map[string]any{"session_id": stringProp("target session ID")}), []string{"session_id", "data", "encoding"}, write), definition("publish_mqtt", "Publish an MQTT message.", mergeMaps(payloadSchema(), map[string]any{"session_id": stringProp("MQTT session ID"), "topic": stringProp("publish topic"), "qos": integerProp("0..2"), "retain": map[string]any{"type": "boolean"}}), []string{"session_id", "topic", "data", "encoding"}, write), definition("subscribe_mqtt", "Subscribe an MQTT session.", map[string]any{"session_id": stringProp("MQTT session ID"), "filter": stringProp("topic filter"), "qos": integerProp("0..2")}, []string{"session_id", "filter"}, write), definition("unsubscribe_mqtt", "Unsubscribe an MQTT session.", map[string]any{"session_id": stringProp("MQTT session ID"), "filter": stringProp("topic filter")}, []string{"session_id", "filter"}, write),
	}
}

func payloadSchema() map[string]any {
	return map[string]any{"data": map[string]any{"type": "string"}, "encoding": map[string]any{"type": "string", "enum": []string{"text", "hex", "base64"}}, "charset": map[string]any{"type": "string", "enum": []string{"utf-8", "ascii", "gbk"}}, "to": map[string]any{"type": "string", "enum": []string{"text", "hex", "base64", "escape", "json-pretty", "json-minify"}}}
}
func convertTool(args map[string]any) (any, error) {
	payload, err := decodeToolPayload(args)
	if err != nil {
		return nil, err
	}
	format := ctconvert.FormatKind(stringArg(args, "to"))
	if format == "" {
		format = ctconvert.FormatHex
	}
	output, err := ctconvert.Encode(payload, ctconvert.Format{Kind: format, Charset: ctconvert.CharsetUTF8})
	return map[string]any{"output": output, "byte_length": len(payload)}, err
}
func decodeToolPayload(args map[string]any) ([]byte, error) {
	data := stringArg(args, "data")
	switch stringArg(args, "encoding") {
	case "text":
		value, err := ctconvert.Decode(ctconvert.PayloadInput{Encoding: ctconvert.EncodingText, Data: data, Charset: ctconvert.Charset(stringArgDefault(args, "charset", "utf-8"))})
		if len(value) > ctmqtt.MaxPayloadBytes {
			return nil, errors.New("payload exceeds 16 MiB")
		}
		return value, err
	case "hex":
		value, err := hex.DecodeString(strings.ReplaceAll(data, " ", ""))
		if len(value) > ctmqtt.MaxPayloadBytes {
			return nil, errors.New("payload exceeds 16 MiB")
		}
		return value, err
	case "base64":
		value, err := base64.StdEncoding.Strict().DecodeString(data)
		if len(value) > ctmqtt.MaxPayloadBytes {
			return nil, errors.New("payload exceeds 16 MiB")
		}
		return value, err
	default:
		return nil, errors.New("encoding must be text, hex, or base64")
	}
}
func publicProfile(value profile.Profile) any {
	endpoint := map[string]any{}
	if value.TCPClient != nil {
		endpoint = map[string]any{"host": value.TCPClient.Host, "port": value.TCPClient.Port}
	}
	if value.TCPServer != nil {
		endpoint = map[string]any{"bind_host": value.TCPServer.BindHost, "port": value.TCPServer.Port}
	}
	if value.MQTT != nil {
		endpoint = map[string]any{"host": value.MQTT.Host, "port": value.MQTT.Port, "version": value.MQTT.Version}
	}
	return map[string]any{"id": value.ID, "name": value.Name, "type": value.Type, "endpoint": endpoint}
}
func recordOutputs(values []records.Record) []any {
	result := make([]any, len(values))
	for i, value := range values {
		result[i] = recordOutput(value)
	}
	return result
}
func recordOutput(value records.Record) any {
	preview := value.Payload
	truncated := false
	if len(preview) > 4096 {
		preview = preview[:4096]
		truncated = true
	}
	return map[string]any{"sequence": value.Sequence, "timestamp": value.Timestamp, "session_id": value.SessionID, "peer_id": value.PeerID, "direction": value.Direction, "payload_base64": base64.StdEncoding.EncodeToString(preview), "payload_length": len(value.Payload), "payload_truncated": truncated, "protocol_meta": value.ProtocolMetadata, "error": value.Error}
}
func stringArg(args map[string]any, key string) string {
	value, _ := args[key].(string)
	return strings.TrimSpace(value)
}
func stringArgDefault(args map[string]any, key, fallback string) string {
	value := stringArg(args, key)
	if value == "" {
		return fallback
	}
	return value
}
func intArg(args map[string]any, key string, fallback int) int {
	value, ok := args[key].(float64)
	if !ok {
		return fallback
	}
	return int(value)
}
func uint64Arg(args map[string]any, key string) uint64 {
	value, ok := args[key].(float64)
	if !ok || value < 0 {
		return 0
	}
	return uint64(value)
}
func boolArg(args map[string]any, key string) bool { value, _ := args[key].(bool); return value }
func isLoopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
func validBearer(header, token string) bool {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	supplied := strings.TrimPrefix(header, prefix)
	return len(supplied) == len(token) && subtle.ConstantTimeCompare([]byte(supplied), []byte(token)) == 1
}
func validHost(host string) bool {
	name, _, err := net.SplitHostPort(host)
	if err != nil {
		name = host
	}
	return isLoopback(strings.Trim(name, "[]"))
}
func validOrigin(origin string) bool {
	return origin == "" || strings.HasPrefix(origin, "http://127.0.0.1:") || strings.HasPrefix(origin, "http://localhost:")
}
func mergeMaps(left, right map[string]any) map[string]any {
	result := map[string]any{}
	for k, v := range left {
		result[k] = v
	}
	for k, v := range right {
		result[k] = v
	}
	return result
}
func mustJSON(value any) string { data, _ := json.Marshal(value); return string(data) }
func writeRPC(writer http.ResponseWriter, response rpcResponse) {
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(response)
}
