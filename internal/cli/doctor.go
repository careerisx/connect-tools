package cli

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"connect-tools/internal/config"
	"github.com/spf13/cobra"
)

const (
	doctorOK         = "ok"
	doctorWarning    = "warning"
	doctorError      = "error"
	doctorNotRunning = "not_running"
)

type doctorCheck struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Detail string `json:"detail"`
}

type doctorOutput struct {
	Healthy bool          `json:"healthy"`
	Checks  []doctorCheck `json:"checks"`
}

func newDoctorCommand(opts *runtimeOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "doctor",
		Short: "Check local paths, credentials, runtime metadata, and service health",
		Args:  noArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runDoctor(cmd, opts)
		},
	}
}

func runDoctor(cmd *cobra.Command, opts *runtimeOptions) error {
	report := doctorOutput{Healthy: true, Checks: make([]doctorCheck, 0, 4)}
	paths, err := config.ResolvePaths()
	if err != nil {
		report.Healthy = false
		report.Checks = append(report.Checks, doctorCheck{
			Name:   "paths",
			Status: doctorError,
			Detail: err.Error(),
		})
		return writeDoctorResult(opts, report, true)
	}
	report.Checks = append(report.Checks, doctorCheck{
		Name:   "paths",
		Status: doctorOK,
		Detail: paths.ConfigDir,
	})

	token, tokenErr := readAccessToken(paths.TokenFile)
	switch {
	case tokenErr == nil:
		report.Checks = append(report.Checks, doctorCheck{Name: "token", Status: doctorOK, Detail: "valid"})
	case errors.Is(tokenErr, os.ErrNotExist):
		report.Checks = append(report.Checks, doctorCheck{Name: "token", Status: doctorWarning, Detail: "not found"})
	default:
		report.Healthy = false
		report.Checks = append(report.Checks, doctorCheck{Name: "token", Status: doctorError, Detail: tokenErr.Error()})
	}

	runtimeInfo, runtimeErr := config.ReadRuntime(paths.RuntimeFile)
	switch {
	case runtimeErr == nil:
		if validationErr := validateRuntimeInfo(runtimeInfo); validationErr != nil {
			report.Healthy = false
			runtimeErr = validationErr
			report.Checks = append(report.Checks, doctorCheck{Name: "runtime", Status: doctorError, Detail: validationErr.Error()})
		} else {
			report.Checks = append(report.Checks, doctorCheck{Name: "runtime", Status: doctorOK, Detail: runtimeInfo.Address})
		}
	case errors.Is(runtimeErr, os.ErrNotExist):
		report.Checks = append(report.Checks, doctorCheck{Name: "runtime", Status: doctorWarning, Detail: "not found"})
	default:
		report.Healthy = false
		report.Checks = append(report.Checks, doctorCheck{Name: "runtime", Status: doctorError, Detail: runtimeErr.Error()})
	}

	if tokenErr == nil && runtimeErr == nil {
		client, clientErr := newAddressClient(runtimeInfo.Address, token, opts.Version)
		if clientErr == nil {
			_, clientErr = getStatus(cmd.Context(), client)
			if cmd.Context().Err() != nil {
				return cmd.Context().Err()
			}
		}
		switch {
		case clientErr == nil:
			report.Checks = append(report.Checks, doctorCheck{Name: "service", Status: doctorOK, Detail: "reachable"})
		case errors.Is(clientErr, ErrServiceNotRunning):
			report.Checks = append(report.Checks, doctorCheck{Name: "service", Status: doctorNotRunning, Detail: "not reachable"})
		default:
			report.Healthy = false
			report.Checks = append(report.Checks, doctorCheck{Name: "service", Status: doctorError, Detail: clientErr.Error()})
		}
	} else {
		report.Checks = append(report.Checks, doctorCheck{Name: "service", Status: doctorNotRunning, Detail: "runtime or token is unavailable"})
	}
	return writeDoctorResult(opts, report, !report.Healthy)
}

func validateRuntimeInfo(info config.RuntimeInfo) error {
	if err := validateClientAddress(info.Address); err != nil {
		return err
	}
	if info.PID <= 0 {
		return errors.New("runtime PID is invalid")
	}
	if info.InstanceID == "" {
		return errors.New("runtime instance ID is empty")
	}
	if info.Version == "" {
		return errors.New("runtime version is empty")
	}
	if info.StartedAt.IsZero() || info.StartedAt.After(time.Now().Add(time.Minute)) {
		return errors.New("runtime start time is invalid")
	}
	return nil
}

func writeDoctorResult(opts *runtimeOptions, report doctorOutput, failed bool) error {
	err := newOutputWriter(opts).write(report, func(writer io.Writer) error {
		for _, check := range report.Checks {
			if err := writeHumanLine(writer, "%s: %s - %s", check.Name, check.Status, check.Detail); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return operationError(err)
	}
	if failed {
		return operationError(fmt.Errorf("doctor found local configuration problems"))
	}
	return nil
}
