package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"connect-tools/internal/config"
	"github.com/spf13/cobra"
)

type statusOutput struct {
	Version    string    `json:"version"`
	InstanceID string    `json:"instance_id"`
	Address    string    `json:"address"`
	StartedAt  time.Time `json:"started_at"`
}

func newStatusCommand(opts *runtimeOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show the running service status",
		Args:  noArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runStatus(cmd.Context(), opts)
		},
	}
}

func runStatus(ctx context.Context, opts *runtimeOptions) error {
	paths, err := config.ResolvePaths()
	if err != nil {
		return operationError(fmt.Errorf("resolve application paths: %w", err))
	}
	client, err := newRuntimeClient(paths, opts.Version)
	if err != nil {
		return err
	}
	status, err := getStatus(ctx, client.core)
	if err != nil {
		return err
	}
	if status.StartedAt == nil {
		return operationError(errors.New("service returned an invalid start time"))
	}
	if err := status.StartedAt.CheckValid(); err != nil {
		return operationError(errors.New("service returned an invalid start time"))
	}

	result := statusOutput{
		Version:    status.Version,
		InstanceID: status.InstanceId,
		Address:    status.ListenAddress,
		StartedAt:  status.StartedAt.AsTime(),
	}
	return newOutputWriter(opts).write(result, func(writer io.Writer) error {
		if err := writeHumanLine(writer, "Version: %s", result.Version); err != nil {
			return err
		}
		if err := writeHumanLine(writer, "Instance ID: %s", result.InstanceID); err != nil {
			return err
		}
		if err := writeHumanLine(writer, "Address: %s", result.Address); err != nil {
			return err
		}
		return writeHumanLine(writer, "Started at: %s", result.StartedAt.Format(time.RFC3339Nano))
	})
}
