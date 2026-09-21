package cli

import (
	"encoding/json"
	"fmt"
	"io"
)

type outputFormat string

const (
	outputHuman outputFormat = "human"
	outputJSON  outputFormat = "json"
)

type outputWriter struct {
	format outputFormat
	writer io.Writer
}

func newOutputWriter(opts *runtimeOptions) outputWriter {
	return outputWriter{
		format: outputFormat(opts.Output),
		writer: opts.Stdout,
	}
}

func (o outputWriter) write(value any, human func(io.Writer) error) error {
	if o.format == outputJSON {
		encoder := json.NewEncoder(o.writer)
		encoder.SetEscapeHTML(false)
		if err := encoder.Encode(value); err != nil {
			return fmt.Errorf("write JSON output: %w", err)
		}
		return nil
	}
	if err := human(o.writer); err != nil {
		return fmt.Errorf("write human output: %w", err)
	}
	return nil
}

func writeHumanLine(writer io.Writer, format string, args ...any) error {
	_, err := fmt.Fprintf(writer, format+"\n", args...)
	return err
}
