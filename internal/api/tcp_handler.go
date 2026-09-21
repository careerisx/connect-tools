package api

import (
	"context"

	connecttoolsv1 "connect-tools/gen/go/connecttools/v1"
	"connect-tools/internal/app"
	"connect-tools/internal/convert"
	"connect-tools/internal/records"
	cttcp "connect-tools/internal/tcp"
	"connectrpc.com/connect"
)

// TCPHandler maps TCPService RPCs to the TCP application service.
type TCPHandler struct {
	service *app.TCPService
}

// NewTCPHandler creates a TCPService handler.
func NewTCPHandler(service *app.TCPService) *TCPHandler { return &TCPHandler{service: service} }

// Send decodes and queues one directed or broadcast TCP payload.
func (h *TCPHandler) Send(ctx context.Context, request *connect.Request[connecttoolsv1.SendTCPRequest]) (*connect.Response[connecttoolsv1.SendTCPResponse], error) {
	if h == nil || h.service == nil || request == nil || request.Msg == nil {
		return nil, invalidArgument("request and TCP service are required")
	}
	id, err := requireID(request.Msg.TargetSessionId, "target session ID")
	if err != nil {
		return nil, invalidArgument(err.Error())
	}
	input, err := payloadFromProto(request.Msg.Payload)
	if err != nil {
		return nil, invalidArgument(err.Error())
	}
	payload, err := convert.Decode(input)
	if err != nil {
		return nil, invalidArgument(err.Error())
	}
	if len(payload) > cttcp.MaxPayloadBytes {
		return nil, connect.NewError(connect.CodeResourceExhausted, cttcp.ErrPayloadTooLarge)
	}
	actor := records.ActorCLI
	if credential, ok := CredentialFromContext(ctx); ok && credential == CredentialBrowserSession {
		actor = records.ActorUser
	}
	result, err := h.service.Send(ctx, id, payload, actor)
	if err != nil {
		return nil, rpcError(err)
	}
	response := &connecttoolsv1.SendTCPResponse{Accepted: result.Accepted, PeerResults: make([]*connecttoolsv1.PeerSendResult, 0, len(result.PeerResults))}
	for _, peer := range result.PeerResults {
		code, message, retryable := peerError(peer.Error)
		response.PeerResults = append(response.PeerResults, &connecttoolsv1.PeerSendResult{
			PeerSessionId: peer.PeerSessionID, Accepted: peer.Accepted, ErrorCode: code, ErrorMessage: message, Retryable: retryable,
		})
	}
	return connect.NewResponse(response), nil
}

// ListPeers returns one page of active peers for a TCP server.
func (h *TCPHandler) ListPeers(ctx context.Context, request *connect.Request[connecttoolsv1.ListPeersRequest]) (*connect.Response[connecttoolsv1.ListPeersResponse], error) {
	if h == nil || h.service == nil || request == nil || request.Msg == nil {
		return nil, invalidArgument("request and TCP service are required")
	}
	id, err := requireID(request.Msg.ServerSessionId, "server session ID")
	if err != nil {
		return nil, invalidArgument(err.Error())
	}
	values, err := h.service.ListPeers(ctx, id)
	if err != nil {
		return nil, rpcError(err)
	}
	start, end, next, err := pageBounds(len(values), request.Msg.PageSize, request.Msg.PageToken)
	if err != nil {
		return nil, invalidArgument(err.Error())
	}
	response := &connecttoolsv1.ListPeersResponse{Peers: make([]*connecttoolsv1.Session, 0, end-start), NextPageToken: next}
	for _, value := range values[start:end] {
		mapped, err := sessionToProto(value)
		if err != nil {
			return nil, rpcError(err)
		}
		response.Peers = append(response.Peers, mapped)
	}
	return connect.NewResponse(response), nil
}
