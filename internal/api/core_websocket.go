package api

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"connect-tools/internal/app"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

const (
	webSocketHandshakeTimeout = 5 * time.Second
	webSocketWriteTimeout     = 10 * time.Second
	webSocketPongTimeout      = 60 * time.Second
	webSocketPingInterval     = 25 * time.Second
	webSocketReadLimit        = 1 << 10
)

// NewCoreWebSocketHandler creates an authenticated live-event transport that
// sends each CoreEvent as one binary Protobuf WebSocket message.
func NewCoreWebSocketHandler(core *app.Core, expectedOrigin string) http.Handler {
	upgrader := websocket.Upgrader{
		HandshakeTimeout: webSocketHandshakeTimeout,
		CheckOrigin: func(request *http.Request) bool {
			return request.Header.Get("Origin") == expectedOrigin
		},
	}

	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		after, err := parseWebSocketCursor(request)
		if err != nil {
			http.Error(response, "invalid event cursor", http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithCancel(request.Context())
		defer cancel()
		events, err := core.Events(ctx, after)
		if err != nil {
			if errors.Is(err, app.ErrTooManySubscribers) {
				http.Error(response, "too many event subscribers", http.StatusServiceUnavailable)
				return
			}
			http.Error(response, "subscribe to core events", http.StatusInternalServerError)
			return
		}

		connection, err := upgrader.Upgrade(response, request, nil)
		if err != nil {
			return
		}
		defer connection.Close()

		connection.SetReadLimit(webSocketReadLimit)
		_ = connection.SetReadDeadline(time.Now().Add(webSocketPongTimeout))
		connection.SetPongHandler(func(string) error {
			return connection.SetReadDeadline(time.Now().Add(webSocketPongTimeout))
		})
		go readWebSocketUntilClosed(connection, cancel)

		ping := time.NewTicker(webSocketPingInterval)
		defer ping.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ping.C:
				if err := writeWebSocketMessage(connection, websocket.PingMessage, nil); err != nil {
					return
				}
			case event, ok := <-events:
				if !ok {
					_ = connection.WriteControl(
						websocket.CloseMessage,
						websocket.FormatCloseMessage(websocket.CloseNormalClosure, "event stream closed"),
						time.Now().Add(webSocketWriteTimeout),
					)
					return
				}
				mapped, err := coreEventToProto(event)
				if err != nil {
					return
				}
				payload, err := proto.Marshal(mapped)
				if err != nil {
					return
				}
				if err := writeWebSocketMessage(connection, websocket.BinaryMessage, payload); err != nil {
					return
				}
			}
		}
	})
}

// parseWebSocketCursor validates the optional last-seen event sequence.
func parseWebSocketCursor(request *http.Request) (uint64, error) {
	value := request.URL.Query().Get("after")
	if value == "" {
		return 0, nil
	}
	return strconv.ParseUint(value, 10, 64)
}

// readWebSocketUntilClosed processes control frames and cancels the event
// subscription when the browser disconnects.
func readWebSocketUntilClosed(connection *websocket.Conn, cancel context.CancelFunc) {
	defer cancel()
	for {
		if _, _, err := connection.ReadMessage(); err != nil {
			return
		}
	}
}

// writeWebSocketMessage applies one bounded write deadline to every frame.
func writeWebSocketMessage(connection *websocket.Conn, messageType int, payload []byte) error {
	if err := connection.SetWriteDeadline(time.Now().Add(webSocketWriteTimeout)); err != nil {
		return err
	}
	return connection.WriteMessage(messageType, payload)
}
