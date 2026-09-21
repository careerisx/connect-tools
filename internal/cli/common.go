package cli

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	connecttoolsv1 "connect-tools/gen/go/connecttools/v1"
	"connect-tools/internal/config"
	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const maxCLIFileBytes = 16 << 20

func loadRuntimeClient(opts *runtimeOptions) (runtimeClient, error) {
	paths, err := config.ResolvePaths()
	if err != nil {
		return runtimeClient{}, operationError(fmt.Errorf("resolve application paths: %w", err))
	}
	client, err := newRuntimeClient(paths, opts.Version)
	if err != nil {
		return runtimeClient{}, classifyCLIError(err)
	}
	return client, nil
}

func classifyCLIError(err error) error {
	if err == nil {
		return nil
	}
	switch connect.CodeOf(err) {
	case connect.CodeInvalidArgument:
		return usageError(err)
	case connect.CodeUnavailable:
		return &commandError{code: exitServiceNotRunning, err: err}
	case connect.CodeDeadlineExceeded:
		return &commandError{code: exitWaitTimeout, err: err}
	default:
		if errors.Is(err, ErrServiceNotRunning) {
			return &commandError{code: exitServiceNotRunning, err: err}
		}
		return operationError(err)
	}
}

func writeProto(opts *runtimeOptions, message proto.Message) error {
	if message == nil {
		return operationError(errors.New("empty service response"))
	}
	marshal := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true, Indent: "  "}
	data, err := marshal.Marshal(message)
	if err != nil {
		return operationError(fmt.Errorf("encode response: %w", err))
	}
	if _, err := opts.Stdout.Write(append(data, '\n')); err != nil {
		return operationError(fmt.Errorf("write response: %w", err))
	}
	return nil
}

func writeProtoCompact(opts *runtimeOptions, message proto.Message) error {
	data, err := (protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}).Marshal(message)
	if err != nil {
		return operationError(fmt.Errorf("encode response: %w", err))
	}
	if _, err := opts.Stdout.Write(append(data, '\n')); err != nil {
		return operationError(err)
	}
	return nil
}

type payloadFlags struct {
	text      string
	hex       string
	base64    string
	file      string
	charset   string
	delimiter string
}

func payloadInputFromFlags(ctx context.Context, cmd flagChanged, opts *runtimeOptions, flags payloadFlags) (*connecttoolsv1.PayloadInput, error) {
	names := []string{"text", "hex", "base64", "file"}
	selected := ""
	for _, name := range names {
		if cmd.Changed(name) {
			if selected != "" {
				return nil, usageErrorf("exactly one of --text, --hex, --base64, or --file is required")
			}
			selected = name
		}
	}
	if selected == "" {
		return nil, usageErrorf("exactly one of --text, --hex, --base64, or --file is required")
	}
	input := &connecttoolsv1.PayloadInput{Charset: flags.charset}
	switch selected {
	case "text":
		input.Encoding = connecttoolsv1.PayloadEncoding_PAYLOAD_ENCODING_TEXT
		input.Data = flags.text
	case "hex":
		if _, err := decodeHex(flags.hex); err != nil {
			return nil, usageError(err)
		}
		input.Encoding = connecttoolsv1.PayloadEncoding_PAYLOAD_ENCODING_HEX
		input.Data = flags.hex
	case "base64":
		if _, err := base64.StdEncoding.Strict().DecodeString(flags.base64); err != nil {
			return nil, usageError(fmt.Errorf("invalid base64 payload: %w", err))
		}
		input.Encoding = connecttoolsv1.PayloadEncoding_PAYLOAD_ENCODING_BASE64
		input.Data = flags.base64
	case "file":
		data, err := readLimitedInput(ctx, opts.Stdin, flags.file)
		if err != nil {
			return nil, err
		}
		input.Encoding = connecttoolsv1.PayloadEncoding_PAYLOAD_ENCODING_BASE64
		input.Data = base64.StdEncoding.EncodeToString(data)
	}
	if flags.delimiter != "" {
		value, err := decodeHex(flags.delimiter)
		if err != nil {
			return nil, usageError(fmt.Errorf("invalid delimiter: %w", err))
		}
		input.AppendDelimiter = value
	}
	return input, nil
}

type flagChanged interface{ Changed(string) bool }

func readLimitedInput(ctx context.Context, stdin io.Reader, path string) ([]byte, error) {
	var reader io.Reader
	var file *os.File
	if path == "-" {
		reader = stdin
	} else {
		opened, err := os.Open(path)
		if err != nil {
			return nil, operationError(fmt.Errorf("open input file: %w", err))
		}
		file = opened
		defer file.Close()
		reader = file
	}
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	data, err := io.ReadAll(io.LimitReader(reader, maxCLIFileBytes+1))
	if err != nil {
		return nil, operationError(fmt.Errorf("read input: %w", err))
	}
	if len(data) > maxCLIFileBytes {
		return nil, usageErrorf("input exceeds %d bytes", maxCLIFileBytes)
	}
	return data, nil
}

func decodeHex(value string) ([]byte, error) {
	clean := strings.NewReplacer(" ", "", "\n", "", "\r", "", "\t", "", "_", "").Replace(value)
	if len(clean)%2 != 0 {
		return nil, errors.New("hex input must contain an even number of digits")
	}
	data, err := hex.DecodeString(clean)
	if err != nil {
		return nil, fmt.Errorf("invalid hex input: %w", err)
	}
	return data, nil
}
