package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
)

const (
	exitSuccess           = 0
	exitArgumentError     = 2
	exitServiceNotRunning = 3
	exitOperationFailed   = 4
	exitWaitTimeout       = 5
)

var (
	// ErrAlreadyRunning indicates that a compatible service already owns the
	// configured local address.
	ErrAlreadyRunning = errors.New("service is already running")
	// ErrServiceNotRunning indicates that no compatible service can be reached.
	ErrServiceNotRunning = errors.New("service is not running")
	// ErrWaitTimeout indicates that a bounded CLI wait expired.
	ErrWaitTimeout = errors.New("wait timed out")
)

type commandError struct {
	code int
	err  error
}

func (e *commandError) Error() string { return e.err.Error() }
func (e *commandError) Unwrap() error { return e.err }

type runtimeOptions struct {
	Version string
	Stdin   io.Reader
	Stdout  io.Writer
	Stderr  io.Writer
	Output  string
}

// Execute runs the CLI with explicitly supplied arguments and streams. It
// returns a stable process exit code and never exits the process itself.
func Execute(ctx context.Context, version string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if ctx == nil {
		ctx = context.Background()
	}
	if version == "" {
		version = "dev"
	}

	opts := &runtimeOptions{
		Version: version,
		Stdin:   stdin,
		Stdout:  stdout,
		Stderr:  stderr,
		Output:  string(outputHuman),
	}
	root := newRootCommand(opts)
	root.SetArgs(args)

	// Cobra's unknown-command error is not typed. Resolve the command first so
	// every command-line shape error still receives the documented exit code.
	if _, _, err := root.Find(args); err != nil {
		return reportError(stderr, usageError(err))
	}
	if err := root.ExecuteContext(ctx); err != nil {
		return reportError(stderr, err)
	}
	return exitSuccess
}

func newRootCommand(opts *runtimeOptions) *cobra.Command {
	root := &cobra.Command{
		Use:           "connect-tools",
		Short:         "Local TCP and MQTT debugging tools",
		SilenceErrors: true,
		SilenceUsage:  true,
		Args:          noArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
		PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
			switch outputFormat(opts.Output) {
			case outputHuman, outputJSON:
				return nil
			default:
				return usageErrorf("invalid output format %q: must be human or json", opts.Output)
			}
		},
	}
	root.SetIn(opts.Stdin)
	root.SetOut(opts.Stdout)
	root.SetErr(opts.Stderr)
	root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return usageError(err)
	})
	root.PersistentFlags().StringVar(&opts.Output, "output", string(outputHuman), "output format: human or json")
	root.AddCommand(
		newServeCommand(opts),
		newStatusCommand(opts),
		newShutdownCommand(opts),
		newDoctorCommand(opts),
		newProfileCommand(opts),
		newSessionCommand(opts),
		newTCPCommand(opts),
		newRecordsCommand(opts),
		newConvertCommand(opts),
		newChecksumCommand(opts),
		newMQTTCommand(opts),
		newMCPCommand(opts),
	)
	return root
}

func noArgs(_ *cobra.Command, args []string) error {
	if len(args) == 0 {
		return nil
	}
	return usageErrorf("unexpected arguments: %s", strings.Join(args, " "))
}

func usageError(err error) error {
	if err == nil {
		return nil
	}
	return &commandError{code: exitArgumentError, err: err}
}

func usageErrorf(format string, args ...any) error {
	return usageError(fmt.Errorf(format, args...))
}

func operationError(err error) error {
	if err == nil {
		return nil
	}
	return &commandError{code: exitOperationFailed, err: err}
}

func reportError(stderr io.Writer, err error) int {
	code := exitCode(err)
	if stderr != nil && err != nil {
		_, _ = fmt.Fprintln(stderr, err)
	}
	return code
}

func exitCode(err error) int {
	if err == nil {
		return exitSuccess
	}
	var coded *commandError
	if errors.As(err, &coded) {
		return coded.code
	}
	if errors.Is(err, ErrServiceNotRunning) {
		return exitServiceNotRunning
	}
	if errors.Is(err, ErrWaitTimeout) || errors.Is(err, context.DeadlineExceeded) {
		return exitWaitTimeout
	}
	return exitOperationFailed
}
