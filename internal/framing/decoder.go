// Package framing splits a TCP byte stream into record-sized byte slices.
// It never infers protocol semantics beyond the explicitly selected mode.
package framing

import (
	"errors"
	"fmt"
)

const (
	// DefaultMaxFrameSize is the default maximum frame and accumulated payload
	// size accepted by a decoder.
	DefaultMaxFrameSize = 16 << 20
)

var (
	// ErrFrameTooLarge indicates that a frame exceeded its configured maximum.
	// Stateful decoders discard their accumulated state before returning it.
	ErrFrameTooLarge = errors.New("frame exceeds configured maximum")
	// ErrInvalidConfig indicates an unsupported mode or invalid framing option.
	ErrInvalidConfig = errors.New("invalid framing configuration")
)

// Mode identifies how bytes read from a TCP connection are grouped.
type Mode string

const (
	// ModeRawRead emits one frame for every non-empty Push call.
	ModeRawRead Mode = "raw-read"
	// ModeFixedLength emits frames of one configured byte length.
	ModeFixedLength Mode = "fixed-length"
	// ModeDelimiter emits frames terminated by an exact byte sequence.
	ModeDelimiter Mode = "delimiter"
)

// Config describes one decoder. A zero MaxFrameSize uses
// DefaultMaxFrameSize.
type Config struct {
	Mode             Mode
	FixedLength      int
	Delimiter        []byte
	IncludeDelimiter bool
	MaxFrameSize     int
}

// Decoder accepts consecutive byte-stream chunks. Returned frames own their
// storage and remain unchanged by later Push or Reset calls. If Push finds an
// oversized later frame after earlier complete frames in the same chunk, it
// returns those complete frames together with ErrFrameTooLarge.
type Decoder interface {
	Push(chunk []byte) (frames [][]byte, err error)
	Reset()
}

// New creates a decoder after validating all mode-specific options.
func New(config Config) (Decoder, error) {
	maximum, err := normalizedMaximum(config.MaxFrameSize)
	if err != nil {
		return nil, err
	}

	switch config.Mode {
	case ModeRawRead:
		return newRaw(maximum), nil
	case ModeFixedLength:
		return newFixed(config.FixedLength, maximum)
	case ModeDelimiter:
		return newDelimiter(config.Delimiter, config.IncludeDelimiter, maximum)
	default:
		return nil, fmt.Errorf("%w: unsupported mode %q", ErrInvalidConfig, config.Mode)
	}
}

// NewRaw creates a raw-read decoder.
func NewRaw(maxFrameSize int) (Decoder, error) {
	return New(Config{Mode: ModeRawRead, MaxFrameSize: maxFrameSize})
}

// NewFixed creates a fixed-length decoder.
func NewFixed(length, maxFrameSize int) (Decoder, error) {
	return New(Config{Mode: ModeFixedLength, FixedLength: length, MaxFrameSize: maxFrameSize})
}

// NewDelimiter creates a delimiter decoder. Delimiter bytes are copied.
func NewDelimiter(delimiter []byte, includeDelimiter bool, maxFrameSize int) (Decoder, error) {
	return New(Config{
		Mode:             ModeDelimiter,
		Delimiter:        delimiter,
		IncludeDelimiter: includeDelimiter,
		MaxFrameSize:     maxFrameSize,
	})
}

func normalizedMaximum(maximum int) (int, error) {
	if maximum == 0 {
		return DefaultMaxFrameSize, nil
	}
	if maximum < 0 {
		return 0, fmt.Errorf("%w: maximum frame size must be positive", ErrInvalidConfig)
	}
	return maximum, nil
}
