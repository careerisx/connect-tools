package cli

import (
	"fmt"

	connecttoolsv1 "connect-tools/gen/go/connecttools/v1"
	"connect-tools/internal/config"
	"connectrpc.com/connect"
	"github.com/spf13/cobra"
)

func newMCPCommand(opts *runtimeOptions) *cobra.Command {
	command := &cobra.Command{Use: "mcp", Short: "Enable MCP and print agent configuration", Args: noArgs}
	command.AddCommand(newMCPSetCommand(opts, true), newMCPSetCommand(opts, false), newMCPStatusCommand(opts), newMCPConfigCommand(opts), newMCPTokenCommand(opts))
	return command
}

func newMCPSetCommand(opts *runtimeOptions, enabled bool) *cobra.Command {
	name := "disable"
	if enabled {
		name = "enable"
	}
	return &cobra.Command{Use: name, Args: noArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		client, err := loadRuntimeClient(opts)
		if err != nil {
			return err
		}
		response, err := client.core.SetMCPEnabled(cmd.Context(), connect.NewRequest(&connecttoolsv1.SetMCPEnabledRequest{Enabled: enabled}))
		if err != nil {
			return classifyCLIError(err)
		}
		return writeProto(opts, response.Msg)
	}}
}

func newMCPStatusCommand(opts *runtimeOptions) *cobra.Command {
	return &cobra.Command{Use: "status", Args: noArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		client, err := loadRuntimeClient(opts)
		if err != nil {
			return err
		}
		status, err := getStatus(cmd.Context(), client.core)
		if err != nil {
			return classifyCLIError(err)
		}
		return writeProto(opts, status)
	}}
}

func newMCPConfigCommand(opts *runtimeOptions) *cobra.Command {
	var agent string
	command := &cobra.Command{Use: "config", Args: noArgs, RunE: func(_ *cobra.Command, _ []string) error {
		// 配置直接携带令牌占位符，由用户通过 `mcp token --show` 获取后手动替换。
		switch agent {
		case "codex":
			_, _ = fmt.Fprintln(opts.Stdout, "[mcp_servers.connect-tools]\nurl = \"http://127.0.0.1:8765/mcp\"\nhttp_headers = { \"Authorization\" = \"Bearer <token>\" }")
		case "claude":
			_, _ = fmt.Fprintln(opts.Stdout, "claude mcp add --transport http connect-tools http://127.0.0.1:8765/mcp --header \"Authorization: Bearer <token>\"")
		case "opencode":
			_, _ = fmt.Fprintln(opts.Stdout, "{\"mcp\":{\"connect-tools\":{\"type\":\"remote\",\"url\":\"http://127.0.0.1:8765/mcp\",\"headers\":{\"Authorization\":\"Bearer <token>\"}}}}")
		default:
			return usageErrorf("--agent must be codex, claude, or opencode")
		}
		return nil
	}}
	command.Flags().StringVar(&agent, "agent", "codex", "codex, claude, or opencode")
	return command
}

func newMCPTokenCommand(opts *runtimeOptions) *cobra.Command {
	var show bool
	command := &cobra.Command{Use: "token", Args: noArgs, RunE: func(_ *cobra.Command, _ []string) error {
		if !show {
			return usageErrorf("token output requires explicit --show")
		}
		paths, err := config.ResolvePaths()
		if err != nil {
			return operationError(err)
		}
		token, err := readAccessToken(paths.TokenFile)
		if err != nil {
			return operationError(err)
		}
		_, err = fmt.Fprintln(opts.Stdout, token)
		return err
	}}
	command.Flags().BoolVar(&show, "show", false, "print the root token to stdout")
	return command
}
