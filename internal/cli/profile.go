package cli

import (
	"io"
	"strings"

	connecttoolsv1 "connect-tools/gen/go/connecttools/v1"
	"connectrpc.com/connect"
	"github.com/spf13/cobra"
)

type profileFlags struct {
	kind, name, description, host, bindHost, frame, delimiter string
	port, fixedLength, maxConnections, keepAliveSeconds       uint32
	autoReconnect, keepAlive, noDelay, includeDelimiter       bool
	mqttVersion, clientID, username                           string
	tls, cleanStart, passwordStdin, keyPassphraseStdin        bool
	caFile, certFile, keyFile, serverName                     string
}

func newProfileCommand(opts *runtimeOptions) *cobra.Command {
	command := &cobra.Command{Use: "profile", Short: "Manage connection profiles", Args: noArgs}
	command.AddCommand(newProfileListCommand(opts), newProfileGetCommand(opts), newProfileCreateCommand(opts), newProfileUpdateCommand(opts), newProfileDeleteCommand(opts))
	return command
}

func newProfileListCommand(opts *runtimeOptions) *cobra.Command {
	var pageSize uint32
	command := &cobra.Command{Use: "list", Args: noArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		client, err := loadRuntimeClient(opts)
		if err != nil {
			return err
		}
		response, err := client.profiles.ListProfiles(cmd.Context(), connect.NewRequest(&connecttoolsv1.ListProfilesRequest{PageSize: pageSize}))
		if err != nil {
			return classifyCLIError(err)
		}
		return writeProto(opts, response.Msg)
	}}
	command.Flags().Uint32Var(&pageSize, "limit", 100, "maximum profiles to return")
	return command
}

func newProfileGetCommand(opts *runtimeOptions) *cobra.Command {
	return &cobra.Command{Use: "get PROFILE_ID", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		client, err := loadRuntimeClient(opts)
		if err != nil {
			return err
		}
		response, err := client.profiles.GetProfile(cmd.Context(), connect.NewRequest(&connecttoolsv1.GetProfileRequest{ProfileId: args[0]}))
		if err != nil {
			return classifyCLIError(err)
		}
		return writeProto(opts, response.Msg)
	}}
}

func newProfileCreateCommand(opts *runtimeOptions) *cobra.Command {
	var flags profileFlags
	command := &cobra.Command{Use: "create", Args: noArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		profile, err := profileFromFlags(flags)
		if err != nil {
			return err
		}
		request := &connecttoolsv1.CreateProfileRequest{Profile: profile}
		if flags.passwordStdin || flags.keyPassphraseStdin {
			secret, readErr := readSecretLine(opts.Stdin)
			if readErr != nil {
				return readErr
			}
			defer clear(secret)
			if flags.passwordStdin {
				request.MqttPassword = append([]byte(nil), secret...)
			} else {
				request.MqttPrivateKeyPassphrase = append([]byte(nil), secret...)
			}
		}
		client, err := loadRuntimeClient(opts)
		if err != nil {
			return err
		}
		response, err := client.profiles.CreateProfile(cmd.Context(), connect.NewRequest(request))
		clear(request.MqttPassword)
		clear(request.MqttPrivateKeyPassphrase)
		if err != nil {
			return classifyCLIError(err)
		}
		return writeProto(opts, response.Msg)
	}}
	bindProfileFlags(command, &flags, true)
	return command
}

func newProfileUpdateCommand(opts *runtimeOptions) *cobra.Command {
	var name, description string
	var clearPassword bool
	command := &cobra.Command{Use: "update PROFILE_ID", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		client, err := loadRuntimeClient(opts)
		if err != nil {
			return err
		}
		current, err := client.profiles.GetProfile(cmd.Context(), connect.NewRequest(&connecttoolsv1.GetProfileRequest{ProfileId: args[0]}))
		if err != nil {
			return classifyCLIError(err)
		}
		if current.Msg == nil || current.Msg.Profile == nil {
			return operationError(io.ErrUnexpectedEOF)
		}
		if cmd.Flags().Changed("name") {
			current.Msg.Profile.Name = name
		}
		if cmd.Flags().Changed("description") {
			current.Msg.Profile.Description = description
		}
		response, err := client.profiles.UpdateProfile(cmd.Context(), connect.NewRequest(&connecttoolsv1.UpdateProfileRequest{Profile: current.Msg.Profile, ClearMqttPassword: clearPassword}))
		if err != nil {
			return classifyCLIError(err)
		}
		return writeProto(opts, response.Msg)
	}}
	command.Flags().StringVar(&name, "name", "", "replacement profile name")
	command.Flags().StringVar(&description, "description", "", "replacement description")
	command.Flags().BoolVar(&clearPassword, "clear-password", false, "delete the MQTT password")
	return command
}

func newProfileDeleteCommand(opts *runtimeOptions) *cobra.Command {
	return &cobra.Command{Use: "delete PROFILE_ID", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		client, err := loadRuntimeClient(opts)
		if err != nil {
			return err
		}
		response, err := client.profiles.DeleteProfile(cmd.Context(), connect.NewRequest(&connecttoolsv1.DeleteProfileRequest{ProfileId: args[0]}))
		if err != nil {
			return classifyCLIError(err)
		}
		return writeProto(opts, response.Msg)
	}}
}

func bindProfileFlags(command *cobra.Command, flags *profileFlags, requireKind bool) {
	command.Flags().StringVar(&flags.kind, "kind", "", "tcp-client, tcp-server, or mqtt")
	command.Flags().StringVar(&flags.name, "name", "", "profile name")
	command.Flags().StringVar(&flags.description, "description", "", "profile description")
	command.Flags().StringVar(&flags.host, "host", "", "remote host")
	command.Flags().StringVar(&flags.bindHost, "bind-host", "127.0.0.1", "server bind host")
	command.Flags().Uint32Var(&flags.port, "port", 0, "TCP or MQTT port")
	command.Flags().StringVar(&flags.frame, "frame", "raw", "raw, fixed, or delimiter")
	command.Flags().Uint32Var(&flags.fixedLength, "fixed-length", 0, "fixed frame length")
	command.Flags().StringVar(&flags.delimiter, "delimiter-hex", "", "frame delimiter as HEX")
	command.Flags().BoolVar(&flags.includeDelimiter, "include-delimiter", false, "include delimiter in received frames")
	command.Flags().Uint32Var(&flags.maxConnections, "max-connections", 128, "maximum TCP server peers")
	command.Flags().BoolVar(&flags.autoReconnect, "auto-reconnect", true, "reconnect TCP/MQTT clients")
	command.Flags().BoolVar(&flags.keepAlive, "keepalive", true, "enable TCP keepalive")
	command.Flags().BoolVar(&flags.noDelay, "no-delay", true, "enable TCP no-delay")
	command.Flags().StringVar(&flags.mqttVersion, "mqtt-version", "5", "MQTT version: 3.1.1 or 5")
	command.Flags().StringVar(&flags.clientID, "client-id", "", "MQTT client ID")
	command.Flags().StringVar(&flags.username, "username", "", "MQTT username")
	command.Flags().Uint32Var(&flags.keepAliveSeconds, "keepalive-seconds", 30, "MQTT keepalive seconds")
	command.Flags().BoolVar(&flags.cleanStart, "clean-start", true, "start a clean MQTT session")
	command.Flags().BoolVar(&flags.passwordStdin, "password-stdin", false, "read MQTT password from stdin")
	command.Flags().BoolVar(&flags.tls, "tls", false, "enable verified TLS")
	command.Flags().StringVar(&flags.caFile, "ca-file", "", "CA bundle path")
	command.Flags().StringVar(&flags.certFile, "client-cert", "", "client certificate path")
	command.Flags().StringVar(&flags.keyFile, "client-key", "", "client private key path")
	command.Flags().StringVar(&flags.serverName, "server-name", "", "TLS verification name")
	command.Flags().BoolVar(&flags.keyPassphraseStdin, "key-passphrase-stdin", false, "read private-key passphrase from stdin")
	if requireKind {
		_ = command.MarkFlagRequired("kind")
		_ = command.MarkFlagRequired("name")
		_ = command.MarkFlagRequired("port")
	}
}

func profileFromFlags(flags profileFlags) (*connecttoolsv1.Profile, error) {
	if strings.TrimSpace(flags.name) == "" {
		return nil, usageErrorf("--name is required")
	}
	if flags.port == 0 || flags.port > 65535 {
		return nil, usageErrorf("--port must be between 1 and 65535")
	}
	profile := &connecttoolsv1.Profile{Name: flags.name, Description: flags.description}
	switch flags.kind {
	case "tcp-client":
		if strings.TrimSpace(flags.host) == "" {
			return nil, usageErrorf("--host is required for tcp-client")
		}
		framing, err := framingFromFlags(flags)
		if err != nil {
			return nil, err
		}
		profile.Connection = &connecttoolsv1.Profile_TcpClient{TcpClient: &connecttoolsv1.TCPClientProfile{Host: flags.host, Port: flags.port, Framing: framing, AutoReconnect: flags.autoReconnect, KeepAlive: flags.keepAlive, NoDelay: flags.noDelay}}
	case "tcp-server":
		framing, err := framingFromFlags(flags)
		if err != nil {
			return nil, err
		}
		profile.Connection = &connecttoolsv1.Profile_TcpServer{TcpServer: &connecttoolsv1.TCPServerProfile{BindHost: flags.bindHost, Port: flags.port, MaxConnections: flags.maxConnections, Framing: framing, KeepAlive: flags.keepAlive, NoDelay: flags.noDelay}}
	case "mqtt":
		if strings.TrimSpace(flags.host) == "" {
			return nil, usageErrorf("--host is required for mqtt")
		}
		version := connecttoolsv1.MQTTVersion_MQTT_VERSION_5_0
		if flags.mqttVersion == "3.1.1" || flags.mqttVersion == "3" {
			version = connecttoolsv1.MQTTVersion_MQTT_VERSION_3_1_1
		} else if flags.mqttVersion != "5" && flags.mqttVersion != "5.0" {
			return nil, usageErrorf("--mqtt-version must be 3.1.1 or 5")
		}
		profile.Connection = &connecttoolsv1.Profile_Mqtt{Mqtt: &connecttoolsv1.MQTTProfile{Version: version, Host: flags.host, Port: flags.port, ClientId: flags.clientID, Username: flags.username, KeepAliveSeconds: flags.keepAliveSeconds, CleanStart: flags.cleanStart, Tls: &connecttoolsv1.MQTTTLSConfig{Enabled: flags.tls, CaFile: flags.caFile, ClientCertFile: flags.certFile, ClientKeyFile: flags.keyFile, ServerName: flags.serverName}, Reconnect: &connecttoolsv1.ReconnectPolicy{Enabled: flags.autoReconnect}}}
	default:
		return nil, usageErrorf("--kind must be tcp-client, tcp-server, or mqtt")
	}
	return profile, nil
}

func framingFromFlags(flags profileFlags) (*connecttoolsv1.FramingConfig, error) {
	result := &connecttoolsv1.FramingConfig{Mode: connecttoolsv1.FrameMode_FRAME_MODE_RAW_READ}
	switch flags.frame {
	case "raw":
	case "fixed":
		if flags.fixedLength == 0 {
			return nil, usageErrorf("--fixed-length is required for fixed framing")
		}
		result.Mode = connecttoolsv1.FrameMode_FRAME_MODE_FIXED_LENGTH
		result.FixedLength = flags.fixedLength
	case "delimiter":
		data, err := decodeHex(flags.delimiter)
		if err != nil || len(data) == 0 {
			return nil, usageErrorf("valid --delimiter-hex is required for delimiter framing")
		}
		result.Mode = connecttoolsv1.FrameMode_FRAME_MODE_DELIMITER
		result.Delimiter = data
		result.IncludeDelimiter = flags.includeDelimiter
	default:
		return nil, usageErrorf("--frame must be raw, fixed, or delimiter")
	}
	return result, nil
}

func readSecretLine(reader io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, 64<<10))
	if err != nil {
		return nil, operationError(err)
	}
	data = []byte(strings.TrimRight(string(data), "\r\n"))
	if len(data) == 0 {
		return nil, usageErrorf("secret input is empty")
	}
	return data, nil
}
