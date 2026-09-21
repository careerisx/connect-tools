package api

import (
	"context"
	"errors"
	"time"

	connecttoolsv1 "connect-tools/gen/go/connecttools/v1"
	"connect-tools/internal/records"
	"connectrpc.com/connect"
)

const (
	defaultRecordWait = 30 * time.Second
	maximumRecordWait = 120 * time.Second
)

// RecordHandler maps RecordService RPCs to the process-owned record store.
type RecordHandler struct {
	store *records.Store
}

// NewRecordHandler creates a RecordService handler.
func NewRecordHandler(store *records.Store) *RecordHandler { return &RecordHandler{store: store} }

// ListRecords returns records after the supplied global sequence cursor.
func (h *RecordHandler) ListRecords(ctx context.Context, request *connect.Request[connecttoolsv1.ListRecordsRequest]) (*connect.Response[connecttoolsv1.ListRecordsResponse], error) {
	if h == nil || h.store == nil || request == nil || request.Msg == nil {
		return nil, invalidArgument("request and record store are required")
	}
	if err := ctx.Err(); err != nil {
		return nil, rpcError(err)
	}
	filter, err := recordFilterFromProto(request.Msg.Filter)
	if err != nil {
		return nil, invalidArgument(err.Error())
	}
	if request.Msg.Cursor != nil {
		filter.After = request.Msg.Cursor.AfterSequence
	}
	if request.Msg.PageSize > records.MaxPageLimit {
		return nil, invalidArgument("page size must not exceed 500")
	}
	filter.Limit = int(request.Msg.PageSize)
	page, err := h.store.List(filter)
	if err != nil {
		return nil, rpcError(err)
	}
	response := &connecttoolsv1.ListRecordsResponse{
		Records:    make([]*connecttoolsv1.DebugRecord, 0, len(page.Records)),
		NextCursor: &connecttoolsv1.RecordCursor{AfterSequence: page.NextCursor}, HasMore: page.HasMore,
	}
	for _, value := range page.Records {
		mapped, err := recordToProto(value)
		if err != nil {
			return nil, rpcError(err)
		}
		response.Records = append(response.Records, mapped)
	}
	return connect.NewResponse(response), nil
}

// WaitForRecord waits for the first historical or future matching record.
func (h *RecordHandler) WaitForRecord(ctx context.Context, request *connect.Request[connecttoolsv1.WaitForRecordRequest]) (*connect.Response[connecttoolsv1.DebugRecord], error) {
	if h == nil || h.store == nil || request == nil || request.Msg == nil {
		return nil, invalidArgument("request and record store are required")
	}
	filter, err := recordFilterFromProto(request.Msg.Filter)
	if err != nil {
		return nil, invalidArgument(err.Error())
	}
	if request.Msg.Cursor != nil {
		filter.After = request.Msg.Cursor.AfterSequence
	}
	timeout := defaultRecordWait
	if request.Msg.TimeoutMs != 0 {
		timeout = time.Duration(request.Msg.TimeoutMs) * time.Millisecond
	}
	if timeout <= 0 || timeout > maximumRecordWait {
		return nil, invalidArgument("timeout must be between 1 and 120000 milliseconds")
	}
	waitContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	value, err := h.store.Wait(waitContext, records.Match{
		SessionIDs: filter.SessionIDs, Direction: filter.Direction, Actor: filter.Actor,
		Contains: filter.Contains, After: filter.After,
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			return nil, connect.NewError(connect.CodeDeadlineExceeded, errors.New("record wait timed out"))
		}
		return nil, rpcError(err)
	}
	mapped, err := recordToProto(value)
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(mapped), nil
}

// ClearRecords removes records matching the supplied filter.
func (h *RecordHandler) ClearRecords(ctx context.Context, request *connect.Request[connecttoolsv1.ClearRecordsRequest]) (*connect.Response[connecttoolsv1.ClearRecordsResponse], error) {
	if h == nil || h.store == nil || request == nil || request.Msg == nil {
		return nil, invalidArgument("request and record store are required")
	}
	if err := ctx.Err(); err != nil {
		return nil, rpcError(err)
	}
	filter, err := recordFilterFromProto(request.Msg.Filter)
	if err != nil {
		return nil, invalidArgument(err.Error())
	}
	result, err := h.store.Clear(filter)
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&connecttoolsv1.ClearRecordsResponse{
		ClearedRecords: result.Records, ClearedPayloadBytes: uint64(result.PayloadBytes),
	}), nil
}
