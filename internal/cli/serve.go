package cli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"time"

	"connect-tools/gen/go/connecttools/v1/connecttoolsv1connect"
	"connect-tools/internal/app"
	"connect-tools/internal/config"
	ctmqtt "connect-tools/internal/mqtt"
	"connect-tools/internal/profile"
	"connect-tools/internal/records"
	"connect-tools/internal/secrets"
	"connect-tools/internal/server"
	"connect-tools/internal/session"
	cttcp "connect-tools/internal/tcp"
	"github.com/spf13/cobra"
)

const (
	instanceIDBytes   = 16
	browserReadyLimit = 5 * time.Second
	browserProbeDelay = 50 * time.Millisecond
)

type serveOutput struct {
	Address    string `json:"address"`
	InstanceID string `json:"instance_id"`
	Existing   bool   `json:"existing"`
}

// newServeCommand 定义服务启动参数，支持指定本地服务访问令牌。
func newServeCommand(opts *runtimeOptions) *cobra.Command {
	var open bool
	// customToken 仅由启动参数提供，不读取或修改系统环境变量。
	var customToken string
	command := &cobra.Command{
		Use:   "serve",
		Short: "Run the local Connect Tools service",
		Args:  noArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if cmd.Flags().Changed("token") {
				if err := config.ValidateToken(customToken); err != nil {
					return usageErrorf("invalid --token: %v", err)
				}
			}
			return runServe(cmd.Context(), opts, open, customToken)
		},
	}
	command.Flags().BoolVar(&open, "open", false, "open the web interface in the default browser")
	command.Flags().StringVar(&customToken, "token", "", "set and persist an access token with at least 32 printable ASCII characters without spaces")
	return command
}

// runServe 启动本地服务，仅在成功监听后持久化显式指定的令牌。
func runServe(ctx context.Context, opts *runtimeOptions, open bool, customToken string) error {
	paths, err := config.ResolvePaths()
	if err != nil {
		return operationError(fmt.Errorf("resolve application paths: %w", err))
	}
	token := customToken
	if token == "" {
		token, err = config.LoadOrCreateToken(paths.TokenFile)
		if err != nil {
			return operationError(fmt.Errorf("load or create service access token: %w", err))
		}
	}

	// 优先使用已保存的令牌探测旧服务，避免更换令牌时误判其运行状态。
	probeToken := token
	if customToken != "" {
		storedToken, readErr := readAccessToken(paths.TokenFile)
		if readErr == nil {
			probeToken = storedToken
		} else if !errors.Is(readErr, os.ErrNotExist) {
			return operationError(fmt.Errorf("load existing service access token: %w", readErr))
		}
	}
	existingClient, err := newAddressClient(server.DefaultAddress, probeToken, opts.Version)
	if err != nil {
		return operationError(fmt.Errorf("configure service probe: %w", err))
	}
	status, probeErr := getStatus(ctx, existingClient)
	if probeErr == nil {
		if customToken != "" && customToken != probeToken {
			return usageErrorf("stop the running service before changing --token")
		}
		// 桌面端只附加到已有服务，不取得它的生命周期所有权。
		if opts.serviceReady != nil {
			opts.serviceReady()
			return nil
		}
		if !open {
			return ErrAlreadyRunning
		}
		openContext, cancelOpen := context.WithTimeout(ctx, browserReadyLimit)
		defer cancelOpen()
		if err := openBrowser(openContext, "http://"+status.ListenAddress+"/"); err != nil {
			if openContext.Err() != nil {
				return openContext.Err()
			}
			return operationError(fmt.Errorf("open browser: %w", err))
		}
		result := serveOutput{Address: status.ListenAddress, InstanceID: status.InstanceId, Existing: true}
		return newOutputWriter(opts).write(result, func(writer io.Writer) error {
			return writeHumanLine(writer, "Opened the running service at %s.", result.Address)
		})
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if !errors.Is(probeErr, ErrServiceNotRunning) {
		return operationError(fmt.Errorf("probe existing service: %w", probeErr))
	}
	// 新实例的浏览器就绪探测必须使用新令牌，而不是旧服务的令牌。
	if probeToken != token {
		existingClient, err = newAddressClient(server.DefaultAddress, token, opts.Version)
		if err != nil {
			return operationError(fmt.Errorf("configure service readiness probe: %w", err))
		}
	}

	instanceID, err := newInstanceID()
	if err != nil {
		return operationError(err)
	}
	startedAt := time.Now().UTC()
	core := app.NewCore(opts.Version, instanceID, server.DefaultAddress, startedAt)
	settings, err := config.LoadSettings(paths.ConfigFile)
	if err != nil {
		return operationError(fmt.Errorf("load settings: %w", err))
	}
	core.SetMCPChangeHook(func(enabled bool) error {
		settings.MCPEnabled = enabled
		return config.SaveSettings(paths.ConfigFile, settings)
	})
	if settings.MCPEnabled {
		if err := core.SetMCPEnabled(true); err != nil {
			return operationError(fmt.Errorf("restore MCP setting: %w", err))
		}
	}
	profileStore := profile.NewFileStore(paths.ProfilesFile)
	secretStore, _ := secrets.Open(ctx)
	sessionManager := session.NewManagerWithObserver(core.ObserveSession)
	recordStore := records.NewDefaultStoreWithObserver(core.ObserveRecord)
	tcpRuntime, err := cttcp.NewRuntime(ctx, sessionManager, recordStore)
	if err != nil {
		return operationError(fmt.Errorf("create TCP runtime: %w", err))
	}
	defer func() { _ = tcpRuntime.Close() }()
	profileService := app.NewProfileServiceWithObserver(profileStore, secretStore, core.ObserveProfile)
	sessionService := app.NewSessionService(profileStore, sessionManager)
	tcpService := app.NewTCPService(profileStore, sessionService, tcpRuntime)
	mqttRuntime, err := ctmqtt.NewRuntime(ctx, ctmqtt.NewFactory(), sessionManager, recordStore, secretStore)
	if err != nil {
		return operationError(fmt.Errorf("create MQTT runtime: %w", err))
	}
	defer func() { _ = mqttRuntime.Close() }()
	mqttService := app.NewMQTTService(profileStore, sessionService, mqttRuntime)
	service, err := server.New(server.Options{
		Address:  server.DefaultAddress,
		Token:    token,
		Core:     core,
		Profiles: profileService,
		Sessions: sessionService,
		TCP:      tcpService,
		MQTT:     mqttService,
		Records:  recordStore,
		Secrets:  secretStore,
	})
	if err != nil {
		return operationError(fmt.Errorf("create service: %w", err))
	}

	browserURL := "http://" + server.DefaultAddress + "/"
	runtimeInfo := config.RuntimeInfo{
		PID:        os.Getpid(),
		Address:    server.DefaultAddress,
		InstanceID: instanceID,
		Version:    opts.Version,
		StartedAt:  startedAt,
	}
	result := serveOutput{Address: server.DefaultAddress, InstanceID: instanceID, Existing: false}
	cancelBrowser := func() {}
	onReady := func() error {
		// 持有监听端口后再原子保存令牌，避免启动冲突覆盖正在运行的服务凭据。
		if customToken != "" {
			if err := config.WritePrivateFile(paths.TokenFile, []byte(token+"\n")); err != nil {
				return operationError(fmt.Errorf("save custom service access token: %w", err))
			}
		}
		if err := config.WriteRuntime(paths.RuntimeFile, runtimeInfo); err != nil {
			return operationError(fmt.Errorf("write service runtime metadata: %w", err))
		}
		if err := newOutputWriter(opts).write(result, func(writer io.Writer) error {
			return writeHumanLine(writer, "Serving on http://%s.", result.Address)
		}); err != nil {
			return operationError(err)
		}
		if opts.serviceReady != nil {
			opts.serviceReady()
		}
		if open {
			openContext, cancelOpen := context.WithTimeout(ctx, browserReadyLimit)
			cancelBrowser = cancelOpen
			go func() {
				defer cancelOpen()
				waitForInstanceAndOpen(openContext, existingClient, instanceID, browserURL, opts.Stderr)
			}()
		}
		return nil
	}
	beforeShutdown := func() error {
		cancelBrowser()
		runtimeErr := errors.Join(tcpRuntime.Close(), mqttRuntime.Close())
		metadataErr := config.RemoveRuntime(paths.RuntimeFile)
		if metadataErr != nil && opts.Stderr != nil {
			_, _ = fmt.Fprintf(opts.Stderr, "remove runtime metadata: %v\n", metadataErr)
		}
		return errors.Join(runtimeErr, metadataErr)
	}
	if err := service.RunWithLifecycleError(ctx, onReady, beforeShutdown); err != nil {
		return operationError(err)
	}
	return nil
}

func newInstanceID() (string, error) {
	raw := make([]byte, instanceIDBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate service instance ID: %w", err)
	}
	return hex.EncodeToString(raw), nil
}

func waitForInstanceAndOpen(ctx context.Context, client connecttoolsv1connect.CoreServiceClient, instanceID, browserURL string, stderr io.Writer) {
	ticker := time.NewTicker(browserProbeDelay)
	defer ticker.Stop()

	for {
		status, err := getStatus(ctx, client)
		if err == nil && status.InstanceId == instanceID {
			if err := openBrowser(ctx, browserURL); err != nil && stderr != nil && !errors.Is(ctx.Err(), context.Canceled) {
				_, _ = fmt.Fprintf(stderr, "open browser: %v\n", err)
			}
			return
		}

		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) && stderr != nil {
				_, _ = fmt.Fprintln(stderr, "open browser: service instance did not become ready before timeout")
			}
			return
		case <-ticker.C:
		}
	}
}

func openBrowser(ctx context.Context, url string) error {
	var command *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		command = exec.CommandContext(ctx, "rundll32", "url.dll,FileProtocolHandler", url)
	case "darwin":
		command = exec.CommandContext(ctx, "open", url)
	case "linux":
		command = exec.CommandContext(ctx, "xdg-open", url)
	default:
		return fmt.Errorf("unsupported operating system %q", runtime.GOOS)
	}
	if err := command.Run(); err != nil {
		return err
	}
	return nil
}
