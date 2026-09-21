package cli

import (
	connecttoolsv1 "connect-tools/gen/go/connecttools/v1"
	"connectrpc.com/connect"
	"github.com/spf13/cobra"
)

func newSessionCommand(opts *runtimeOptions) *cobra.Command {
	command := &cobra.Command{Use: "session", Short: "Manage runtime sessions", Args: noArgs}
	command.AddCommand(
		&cobra.Command{Use: "list", Args: noArgs, RunE: func(cmd *cobra.Command, _ []string) error {
			client, err := loadRuntimeClient(opts)
			if err != nil {
				return err
			}
			response, err := client.sessions.ListSessions(cmd.Context(), connect.NewRequest(&connecttoolsv1.ListSessionsRequest{PageSize: 500}))
			if err != nil {
				return classifyCLIError(err)
			}
			return writeProto(opts, response.Msg)
		}},
		&cobra.Command{Use: "get SESSION_ID", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
			client, err := loadRuntimeClient(opts)
			if err != nil {
				return err
			}
			response, err := client.sessions.GetSession(cmd.Context(), connect.NewRequest(&connecttoolsv1.GetSessionRequest{SessionId: args[0]}))
			if err != nil {
				return classifyCLIError(err)
			}
			return writeProto(opts, response.Msg)
		}},
		newSessionStartCommand(opts),
		&cobra.Command{Use: "stop SESSION_ID", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
			client, err := loadRuntimeClient(opts)
			if err != nil {
				return err
			}
			response, err := client.sessions.StopSession(cmd.Context(), connect.NewRequest(&connecttoolsv1.StopSessionRequest{SessionId: args[0]}))
			if err != nil {
				return classifyCLIError(err)
			}
			return writeProto(opts, response.Msg)
		}},
	)
	return command
}

func newSessionStartCommand(opts *runtimeOptions) *cobra.Command {
	var displayName string
	command := &cobra.Command{Use: "start PROFILE_ID", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		request := &connecttoolsv1.StartSessionRequest{ProfileId: args[0]}
		if cmd.Flags().Changed("display-name") {
			request.DisplayName = &displayName
		}
		client, err := loadRuntimeClient(opts)
		if err != nil {
			return err
		}
		response, err := client.sessions.StartSession(cmd.Context(), connect.NewRequest(request))
		if err != nil {
			return classifyCLIError(err)
		}
		return writeProto(opts, response.Msg)
	}}
	command.Flags().StringVar(&displayName, "display-name", "", "session display name")
	return command
}
