package cli

import (
	"context"
	"strings"

	connecttoolsv1 "connect-tools/gen/go/connecttools/v1"
	"connectrpc.com/connect"
	"github.com/spf13/cobra"
)

type recordFlags struct {
	sessions                                    []string
	direction, actor, containsText, containsHex string
	after                                       uint64
	limit, timeout                              uint32
}

func newRecordsCommand(opts *runtimeOptions) *cobra.Command {
	command := &cobra.Command{Use: "records", Short: "List, wait for, follow, or clear records", Args: noArgs}
	command.AddCommand(newRecordsListCommand(opts), newRecordsWaitCommand(opts), newRecordsFollowCommand(opts), newRecordsClearCommand(opts))
	return command
}

func newRecordsListCommand(opts *runtimeOptions) *cobra.Command {
	var flags recordFlags
	command := &cobra.Command{Use: "list", Args: noArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		filter, err := recordFilter(flags)
		if err != nil {
			return err
		}
		client, err := loadRuntimeClient(opts)
		if err != nil {
			return err
		}
		response, err := client.records.ListRecords(cmd.Context(), connect.NewRequest(&connecttoolsv1.ListRecordsRequest{Filter: filter, Cursor: &connecttoolsv1.RecordCursor{AfterSequence: flags.after}, PageSize: flags.limit}))
		if err != nil {
			return classifyCLIError(err)
		}
		return writeProto(opts, response.Msg)
	}}
	bindRecordFlags(command, &flags)
	command.Flags().Uint32Var(&flags.limit, "limit", 100, "maximum records")
	return command
}

func newRecordsWaitCommand(opts *runtimeOptions) *cobra.Command {
	var flags recordFlags
	command := &cobra.Command{Use: "wait", Args: noArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		filter, err := recordFilter(flags)
		if err != nil {
			return err
		}
		if flags.timeout == 0 || flags.timeout > 120000 {
			return usageErrorf("--timeout-ms must be between 1 and 120000")
		}
		client, err := loadRuntimeClient(opts)
		if err != nil {
			return err
		}
		response, err := client.records.WaitForRecord(cmd.Context(), connect.NewRequest(&connecttoolsv1.WaitForRecordRequest{Filter: filter, Cursor: &connecttoolsv1.RecordCursor{AfterSequence: flags.after}, TimeoutMs: flags.timeout}))
		if err != nil {
			return classifyCLIError(err)
		}
		return writeProto(opts, response.Msg)
	}}
	bindRecordFlags(command, &flags)
	command.Flags().Uint32Var(&flags.timeout, "timeout-ms", 30000, "wait timeout in milliseconds")
	return command
}

func newRecordsFollowCommand(opts *runtimeOptions) *cobra.Command {
	var flags recordFlags
	command := &cobra.Command{Use: "follow", Args: noArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		filter, err := recordFilter(flags)
		if err != nil {
			return err
		}
		client, err := loadRuntimeClient(opts)
		if err != nil {
			return err
		}
		cursor := flags.after
		for {
			response, callErr := client.records.WaitForRecord(cmd.Context(), connect.NewRequest(&connecttoolsv1.WaitForRecordRequest{Filter: filter, Cursor: &connecttoolsv1.RecordCursor{AfterSequence: cursor}, TimeoutMs: 120000}))
			if callErr != nil {
				if cmd.Context().Err() != nil || connect.CodeOf(callErr) == connect.CodeDeadlineExceeded {
					if cmd.Context().Err() != nil {
						return nil
					}
					continue
				}
				return classifyCLIError(callErr)
			}
			if response.Msg == nil {
				return operationError(context.Canceled)
			}
			cursor = response.Msg.Sequence
			if err := writeProtoCompact(opts, response.Msg); err != nil {
				return err
			}
		}
	}}
	bindRecordFlags(command, &flags)
	return command
}

func newRecordsClearCommand(opts *runtimeOptions) *cobra.Command {
	var flags recordFlags
	var all bool
	command := &cobra.Command{Use: "clear", Args: noArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		filter, err := recordFilter(flags)
		if err != nil {
			return err
		}
		if !all && len(flags.sessions) == 0 && flags.direction == "" && flags.actor == "" && flags.containsText == "" && flags.containsHex == "" {
			return usageErrorf("use --all or provide a filter")
		}
		client, err := loadRuntimeClient(opts)
		if err != nil {
			return err
		}
		response, err := client.records.ClearRecords(cmd.Context(), connect.NewRequest(&connecttoolsv1.ClearRecordsRequest{Filter: filter}))
		if err != nil {
			return classifyCLIError(err)
		}
		return writeProto(opts, response.Msg)
	}}
	bindRecordFlags(command, &flags)
	command.Flags().BoolVar(&all, "all", false, "clear all retained records")
	return command
}

func bindRecordFlags(command *cobra.Command, flags *recordFlags) {
	command.Flags().StringSliceVar(&flags.sessions, "session", nil, "session ID; repeatable")
	command.Flags().StringVar(&flags.direction, "direction", "", "inbound, outbound, or internal")
	command.Flags().StringVar(&flags.actor, "actor", "", "remote, user, cli, mcp, or system")
	command.Flags().StringVar(&flags.containsText, "contains-text", "", "raw UTF-8 payload substring")
	command.Flags().StringVar(&flags.containsHex, "contains-hex", "", "raw HEX payload substring")
	command.Flags().Uint64Var(&flags.after, "after", 0, "exclusive record sequence cursor")
}

func recordFilter(flags recordFlags) (*connecttoolsv1.RecordFilter, error) {
	filter := &connecttoolsv1.RecordFilter{SessionIds: flags.sessions}
	if flags.direction != "" {
		value := connecttoolsv1.Direction_DIRECTION_UNSPECIFIED
		switch strings.ToLower(flags.direction) {
		case "inbound":
			value = connecttoolsv1.Direction_DIRECTION_INBOUND
		case "outbound":
			value = connecttoolsv1.Direction_DIRECTION_OUTBOUND
		case "internal":
			value = connecttoolsv1.Direction_DIRECTION_INTERNAL
		default:
			return nil, usageErrorf("invalid direction")
		}
		filter.Direction = &value
	}
	if flags.actor != "" {
		value := connecttoolsv1.Actor_ACTOR_UNSPECIFIED
		switch strings.ToLower(flags.actor) {
		case "remote":
			value = connecttoolsv1.Actor_ACTOR_REMOTE
		case "user":
			value = connecttoolsv1.Actor_ACTOR_USER
		case "cli":
			value = connecttoolsv1.Actor_ACTOR_CLI
		case "mcp":
			value = connecttoolsv1.Actor_ACTOR_MCP
		case "system":
			value = connecttoolsv1.Actor_ACTOR_SYSTEM
		default:
			return nil, usageErrorf("invalid actor")
		}
		filter.Actor = &value
	}
	if flags.containsText != "" && flags.containsHex != "" {
		return nil, usageErrorf("--contains-text and --contains-hex are mutually exclusive")
	}
	if flags.containsText != "" {
		filter.PayloadContains = []byte(flags.containsText)
	}
	if flags.containsHex != "" {
		value, err := decodeHex(flags.containsHex)
		if err != nil {
			return nil, usageError(err)
		}
		filter.PayloadContains = value
	}
	return filter, nil
}
