// Package desktop 将现有 Web UI 和本地服务装入 Wails 桌面窗口。
package desktop

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"

	"connect-tools/internal/cli"
	"connect-tools/internal/server"
	"connect-tools/internal/web"
	"github.com/gorilla/websocket"
	"github.com/wailsapp/wails/v2"
	"github.com/wailsapp/wails/v2/pkg/options"
	"github.com/wailsapp/wails/v2/pkg/options/assetserver"
	wailsruntime "github.com/wailsapp/wails/v2/pkg/runtime"
)

const (
	// transportPath 仅由 Wails 资源服务提供，不暴露给独立的本地 API 服务。
	transportPath = "/desktop/transport"
	// bridgeTokenBytes 为每个桌面窗口生成独立的临时 WebSocket 访问凭据。
	bridgeTokenBytes = 32
	// bridgeTimeout 限制 HTTP 请求头与后端 WebSocket 握手的等待时间。
	bridgeTimeout = 5 * time.Second
)

// Run 创建原生窗口，窗口退出时清理桌面转接和本次启动的本地服务。
func Run(ctx context.Context, version string) (resultErr error) {
	bridgeContext, cancelBridge := context.WithCancel(ctx)
	defer cancelBridge()
	// 延后业务服务启动，避免 Wails 在打包生成绑定时启动连接或修改运行信息。
	var service *cli.DesktopService
	var serviceErr error
	var startOnce sync.Once
	prepareService := func() error {
		startOnce.Do(func() {
			service, serviceErr = cli.StartDesktopService(bridgeContext, version)
		})
		return serviceErr
	}
	defer func() {
		cancelBridge()
		// 等待已开始的初始化，并阻止窗口退出后的新请求再次创建服务。
		startOnce.Do(func() {})
		if service != nil {
			resultErr = errors.Join(resultErr, service.Close())
		} else {
			resultErr = errors.Join(resultErr, serviceErr)
		}
	}()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("创建桌面事件转接失败: %w", err)
	}
	defer listener.Close()
	secret := make([]byte, bridgeTokenBytes)
	if _, err := rand.Read(secret); err != nil {
		return fmt.Errorf("生成桌面临时凭据失败: %w", err)
	}
	token := hex.EncodeToString(secret)
	backend := &url.URL{Scheme: "http", Host: server.DefaultAddress}
	eventURL := url.URL{Scheme: "ws", Host: listener.Addr().String(), Path: "/ws/events"}
	query := eventURL.Query()
	query.Set("token", token)
	eventURL.RawQuery = query.Encode()

	// 系统 WebView 的资源处理器不支持 WebSocket Hijack，使用临时回环端口转接。
	// 原服务仍只接受原本的同源请求；桌面端通过独立随机凭据访问转接端口。
	bridge := &http.Server{
		Handler:           eventBridge(bridgeContext, backend, token),
		ReadHeaderTimeout: bridgeTimeout,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    32 << 10,
	}
	bridgeDone := make(chan error, 1)
	go func() { bridgeDone <- bridge.Serve(listener) }()
	defer func() {
		cancelBridge()
		closeErr := bridge.Close()
		serveErr := <-bridgeDone
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
		resultErr = errors.Join(resultErr, closeErr, serveErr)
	}()

	return wails.Run(&options.App{
		Title:     "连接工具",
		Width:     1440,
		Height:    900,
		MinWidth:  960,
		MinHeight: 640,
		AssetServer: &assetserver.Options{
			Assets:  web.Assets(),
			Handler: transportHandler(backend, eventURL.String(), prepareService),
		},
		OnStartup: func(windowContext context.Context) {
			if err := prepareService(); err != nil {
				_, _ = wailsruntime.MessageDialog(windowContext, wailsruntime.MessageDialogOptions{
					Type:    wailsruntime.ErrorDialog,
					Title:   "连接工具启动失败",
					Message: err.Error(),
				})
				wailsruntime.Quit(windowContext)
				return
			}
			// 系统退出信号也应结束原生消息循环，随后执行统一清理。
			go func() {
				select {
				case <-ctx.Done():
					wailsruntime.Quit(windowContext)
				case <-bridgeContext.Done():
				}
			}()
		},
		OnShutdown: func(context.Context) { cancelBridge() },
	})
}

// transportHandler 提供桌面事件地址，并将原有 Connect RPC 请求转发到本地服务。
func transportHandler(backend *url.URL, eventURL string, prepareService func() error) http.Handler {
	proxy := httputil.NewSingleHostReverseProxy(backend)
	proxy.Transport = &http.Transport{Proxy: nil}
	proxy.ErrorHandler = func(response http.ResponseWriter, _ *http.Request, _ error) {
		http.Error(response, "本地服务不可用", http.StatusBadGateway)
	}
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path == transportPath || strings.HasPrefix(request.URL.Path, "/connecttools.v1.") {
			if err := prepareService(); err != nil {
				http.Error(response, "本地服务启动失败", http.StatusServiceUnavailable)
				return
			}
		}
		if request.URL.Path == transportPath && request.Method == http.MethodGet {
			response.Header().Set("Content-Type", "application/json; charset=utf-8")
			response.Header().Set("Cache-Control", "no-store")
			_ = json.NewEncoder(response).Encode(map[string]string{"eventURL": eventURL})
			return
		}
		if strings.HasPrefix(request.URL.Path, "/connecttools.v1.") {
			clone := request.Clone(request.Context())
			clone.Host = backend.Host
			clone.Header.Set("Origin", backend.String())
			proxy.ServeHTTP(response, clone)
			return
		}
		http.NotFound(response, request)
	})
}

// eventBridge 校验窗口临时凭据后，双向转接原有的二进制 Protobuf 事件流。
func eventBridge(ctx context.Context, backend *url.URL, token string) http.Handler {
	upgrader := websocket.Upgrader{
		HandshakeTimeout: bridgeTimeout,
		// 自定义 WebView 协议可能发送空 Origin，访问控制由窗口独享的随机凭据承担。
		CheckOrigin: func(*http.Request) bool { return true },
	}
	dialer := websocket.Dialer{HandshakeTimeout: bridgeTimeout}
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/ws/events" {
			http.NotFound(response, request)
			return
		}
		if subtle.ConstantTimeCompare([]byte(request.URL.Query().Get("token")), []byte(token)) != 1 {
			http.Error(response, "forbidden", http.StatusForbidden)
			return
		}
		target := url.URL{Scheme: "ws", Host: backend.Host, Path: "/ws/events"}
		query := target.Query()
		query.Set("after", request.URL.Query().Get("after"))
		target.RawQuery = query.Encode()
		outgoing, handshake, err := dialer.DialContext(ctx, target.String(), http.Header{"Origin": {backend.String()}})
		if err != nil {
			if handshake != nil && handshake.Body != nil {
				_ = handshake.Body.Close()
			}
			http.Error(response, "本地事件服务不可用", http.StatusBadGateway)
			return
		}
		defer outgoing.Close()
		incoming, err := upgrader.Upgrade(response, request, nil)
		if err != nil {
			return
		}
		defer incoming.Close()
		// 与原服务的限制保持一致，并为二进制事件元数据保留空间。
		incoming.SetReadLimit(1 << 10)
		outgoing.SetReadLimit(24 << 20)
		finished := make(chan struct{}, 2)
		go forwardMessages(outgoing, incoming, finished)
		go forwardMessages(incoming, outgoing, finished)
		select {
		case <-finished:
		case <-ctx.Done():
		case <-request.Context().Done():
		}
	})
}

// forwardMessages 保持 WebSocket 数据帧类型和内容，读写失败时通知连接所有者清理。
func forwardMessages(destination, source *websocket.Conn, finished chan<- struct{}) {
	defer func() { finished <- struct{}{} }()
	for {
		messageType, payload, err := source.ReadMessage()
		if err != nil {
			return
		}
		if err := destination.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
			return
		}
		if err := destination.WriteMessage(messageType, payload); err != nil {
			return
		}
	}
}
