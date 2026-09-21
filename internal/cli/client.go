package cli

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"

	connecttoolsv1 "connect-tools/gen/go/connecttools/v1"
	"connect-tools/gen/go/connecttools/v1/connecttoolsv1connect"
	"connect-tools/internal/config"
	"connectrpc.com/connect"
)

const (
	accessTokenHexLength = 64
)

type runtimeClient struct {
	info     config.RuntimeInfo
	core     connecttoolsv1connect.CoreServiceClient
	profiles connecttoolsv1connect.ProfileServiceClient
	sessions connecttoolsv1connect.SessionServiceClient
	records  connecttoolsv1connect.RecordServiceClient
	tcp      connecttoolsv1connect.TCPServiceClient
	mqtt     connecttoolsv1connect.MQTTServiceClient
}

type authorizationTransport struct {
	base      http.RoundTripper
	token     string
	userAgent string
}

func (t authorizationTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	clone := request.Clone(request.Context())
	clone.Header = request.Header.Clone()
	clone.Header.Set("Authorization", "Bearer "+t.token)
	clone.Header.Set("User-Agent", t.userAgent)
	return t.base.RoundTrip(clone)
}

func newRuntimeClient(paths config.Paths, version string) (runtimeClient, error) {
	info, err := config.ReadRuntime(paths.RuntimeFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return runtimeClient{}, ErrServiceNotRunning
		}
		return runtimeClient{}, fmt.Errorf("load service runtime metadata: %w", err)
	}
	token, err := readAccessToken(paths.TokenFile)
	if err != nil {
		return runtimeClient{}, fmt.Errorf("load service access token: %w", err)
	}
	base, httpClient, err := newAddressHTTPClient(info.Address, token, version)
	if err != nil {
		return runtimeClient{}, fmt.Errorf("configure service client: %w", err)
	}
	return runtimeClient{
		info:     info,
		core:     connecttoolsv1connect.NewCoreServiceClient(httpClient, base),
		profiles: connecttoolsv1connect.NewProfileServiceClient(httpClient, base),
		sessions: connecttoolsv1connect.NewSessionServiceClient(httpClient, base),
		records:  connecttoolsv1connect.NewRecordServiceClient(httpClient, base),
		tcp:      connecttoolsv1connect.NewTCPServiceClient(httpClient, base),
		mqtt:     connecttoolsv1connect.NewMQTTServiceClient(httpClient, base),
	}, nil
}

func newAddressClient(address, token, version string) (connecttoolsv1connect.CoreServiceClient, error) {
	base, client, err := newAddressHTTPClient(address, token, version)
	if err != nil {
		return nil, err
	}
	return connecttoolsv1connect.NewCoreServiceClient(client, base), nil
}

func newAddressHTTPClient(address, token, version string) (string, *http.Client, error) {
	if err := validateClientAddress(address); err != nil {
		return "", nil, err
	}
	if token == "" {
		return "", nil, errors.New("access token is empty")
	}
	if version == "" {
		version = "dev"
	}

	transport := authorizationTransport{
		base:      http.DefaultTransport,
		token:     token,
		userAgent: "connect-tools-cli/" + version,
	}
	httpClient := &http.Client{
		Transport: transport,
	}
	return "http://" + address, httpClient, nil
}

func readAccessToken(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read access token file %q: %w", path, err)
	}
	contents := string(data)
	token := contents
	if len(contents) == accessTokenHexLength+1 && strings.HasSuffix(contents, "\n") {
		token = strings.TrimSuffix(contents, "\n")
	}
	if len(token) != accessTokenHexLength || (contents != token && contents != token+"\n") {
		return "", fmt.Errorf("invalid access token file %q", path)
	}
	if _, err := hex.DecodeString(token); err != nil {
		return "", fmt.Errorf("invalid access token file %q", path)
	}
	return token, nil
}

func validateClientAddress(address string) error {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid runtime address %q: %w", address, err)
	}
	if host != "127.0.0.1" {
		return errors.New("runtime address must use 127.0.0.1")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("invalid runtime port %q", portText)
	}
	return nil
}

func getStatus(ctx context.Context, client connecttoolsv1connect.CoreServiceClient) (*connecttoolsv1.CoreStatus, error) {
	response, err := client.GetStatus(ctx, connect.NewRequest(&connecttoolsv1.GetStatusRequest{}))
	if err != nil {
		return nil, classifyRPCError(ctx, err)
	}
	return response.Msg, nil
}

func classifyRPCError(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if connect.CodeOf(err) == connect.CodeUnavailable {
		return fmt.Errorf("%w: local service is unreachable", ErrServiceNotRunning)
	}
	return err
}
