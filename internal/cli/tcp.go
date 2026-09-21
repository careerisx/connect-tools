package cli

import (
	connecttoolsv1 "connect-tools/gen/go/connecttools/v1"
	"connectrpc.com/connect"
	"github.com/spf13/cobra"
)

func newTCPCommand(opts *runtimeOptions) *cobra.Command {
	command := &cobra.Command{Use: "tcp", Short: "Send TCP data and inspect peers", Args: noArgs}
	command.AddCommand(newTCPSendCommand(opts), newTCPPeersCommand(opts))
	return command
}

func newTCPSendCommand(opts *runtimeOptions) *cobra.Command {
	var flags payloadFlags
	command := &cobra.Command{Use: "send SESSION_ID", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		payload, err := payloadInputFromFlags(cmd.Context(), cmd.Flags(), opts, flags)
		if err != nil {
			return err
		}
		client, err := loadRuntimeClient(opts)
		if err != nil {
			return err
		}
		response, err := client.tcp.Send(cmd.Context(), connect.NewRequest(&connecttoolsv1.SendTCPRequest{TargetSessionId: args[0], Payload: payload}))
		if err != nil {
			return classifyCLIError(err)
		}
		return writeProto(opts, response.Msg)
	}}
	bindPayloadFlags(command, &flags)
	return command
}

func newTCPPeersCommand(opts *runtimeOptions) *cobra.Command {
	return &cobra.Command{Use: "peers SERVER_SESSION_ID", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		client, err := loadRuntimeClient(opts)
		if err != nil {
			return err
		}
		response, err := client.tcp.ListPeers(cmd.Context(), connect.NewRequest(&connecttoolsv1.ListPeersRequest{ServerSessionId: args[0], PageSize: 500}))
		if err != nil {
			return classifyCLIError(err)
		}
		return writeProto(opts, response.Msg)
	}}
}

func bindPayloadFlags(command *cobra.Command, flags *payloadFlags) {
	command.Flags().StringVar(&flags.text, "text", "", "text payload")
	command.Flags().StringVar(&flags.hex, "hex", "", "HEX payload")
	command.Flags().StringVar(&flags.base64, "base64", "", "Base64 payload")
	command.Flags().StringVar(&flags.file, "file", "", "payload file, or - for stdin")
	command.Flags().StringVar(&flags.charset, "charset", "utf-8", "text charset: utf-8, ascii, or gbk")
	command.Flags().StringVar(&flags.delimiter, "append-delimiter-hex", "", "append HEX bytes after decoded payload")
}
