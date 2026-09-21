package cli

import (
	"context"
	"fmt"
	"io"

	connecttoolsv1 "connect-tools/gen/go/connecttools/v1"
	"connect-tools/internal/config"
	"connectrpc.com/connect"
	"github.com/spf13/cobra"
)

type shutdownOutput struct {
	Accepted bool `json:"accepted"`
}

func newShutdownCommand(opts *runtimeOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "shutdown",
		Short: "Request graceful service shutdown",
		Args:  noArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runShutdown(cmd.Context(), opts)
		},
	}
}

func runShutdown(ctx context.Context, opts *runtimeOptions) error {
	paths, err := config.ResolvePaths()
	if err != nil {
		return operationError(fmt.Errorf("resolve application paths: %w", err))
	}
	client, err := newRuntimeClient(paths, opts.Version)
	if err != nil {
		return err
	}
	response, err := client.core.Shutdown(ctx, connect.NewRequest(&connecttoolsv1.ShutdownRequest{}))
	if err != nil {
		return classifyRPCError(ctx, err)
	}

	result := shutdownOutput{Accepted: response.Msg.Accepted}
	return newOutputWriter(opts).write(result, func(writer io.Writer) error {
		if result.Accepted {
			return writeHumanLine(writer, "Shutdown requested.")
		}
		return writeHumanLine(writer, "Shutdown was already in progress.")
	})
}
