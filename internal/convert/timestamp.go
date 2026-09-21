package convert

import (
	"errors"
	"fmt"
	"math"
	"time"
)

var (
	// ErrInvalidTimestamp indicates an unsupported timestamp unit.
	ErrInvalidTimestamp = errors.New("invalid timestamp")
	// ErrTimestampOverflow indicates that a timestamp cannot fit in int64 for
	// the requested unit.
	ErrTimestampOverflow = errors.New("timestamp overflows int64")
)

// TimestampUnit identifies an integer Unix timestamp resolution.
type TimestampUnit string

const (
	// TimestampSeconds counts seconds since the Unix epoch.
	TimestampSeconds TimestampUnit = "seconds"
	// TimestampMilliseconds counts milliseconds since the Unix epoch.
	TimestampMilliseconds TimestampUnit = "milliseconds"
	// TimestampMicroseconds counts microseconds since the Unix epoch.
	TimestampMicroseconds TimestampUnit = "microseconds"
	// TimestampNanoseconds counts nanoseconds since the Unix epoch.
	TimestampNanoseconds TimestampUnit = "nanoseconds"
)

// UnixToTime converts an int64 Unix timestamp to UTC.
func UnixToTime(value int64, unit TimestampUnit) (time.Time, error) {
	var seconds, nanoseconds int64
	switch unit {
	case TimestampSeconds:
		seconds = value
	case TimestampMilliseconds:
		seconds = value / 1_000
		nanoseconds = value % 1_000 * int64(time.Millisecond)
	case TimestampMicroseconds:
		seconds = value / 1_000_000
		nanoseconds = value % 1_000_000 * int64(time.Microsecond)
	case TimestampNanoseconds:
		seconds = value / 1_000_000_000
		nanoseconds = value % 1_000_000_000
	default:
		return time.Time{}, fmt.Errorf("%w: unsupported unit %q", ErrInvalidTimestamp, unit)
	}
	normalizedSeconds := seconds
	normalizedNanoseconds := nanoseconds
	if normalizedNanoseconds < 0 {
		normalizedSeconds--
		normalizedNanoseconds += int64(time.Second)
	}
	result := time.Unix(seconds, nanoseconds).UTC()
	if result.Unix() != normalizedSeconds || int64(result.Nanosecond()) != normalizedNanoseconds {
		return time.Time{}, ErrTimestampOverflow
	}
	return result, nil
}

// TimeToUnix converts a time to an int64 Unix timestamp at the selected
// resolution. Sub-unit precision is truncated toward the preceding Unix tick,
// matching time.Time Unix methods.
func TimeToUnix(value time.Time, unit TimestampUnit) (int64, error) {
	seconds := value.Unix()
	nanoseconds := int64(value.Nanosecond())
	if !time.Unix(seconds, nanoseconds).Equal(value) {
		return 0, ErrTimestampOverflow
	}
	switch unit {
	case TimestampSeconds:
		return seconds, nil
	case TimestampMilliseconds:
		return scaledUnix(seconds, 1_000, nanoseconds/int64(time.Millisecond))
	case TimestampMicroseconds:
		return scaledUnix(seconds, 1_000_000, nanoseconds/int64(time.Microsecond))
	case TimestampNanoseconds:
		return scaledUnix(seconds, 1_000_000_000, nanoseconds)
	default:
		return 0, fmt.Errorf("%w: unsupported unit %q", ErrInvalidTimestamp, unit)
	}
}

func scaledUnix(seconds, scale, remainder int64) (int64, error) {
	maximumQuotient := int64(math.MaxInt64) / scale
	if seconds > maximumQuotient || seconds == maximumQuotient && remainder > int64(math.MaxInt64)%scale {
		return 0, ErrTimestampOverflow
	}
	minimumQuotient := int64(math.MinInt64) / scale
	minimumRemainder := int64(math.MinInt64) % scale
	if seconds < minimumQuotient {
		minimumRescue := scale + minimumRemainder
		if seconds != minimumQuotient-1 || remainder < minimumRescue {
			return 0, ErrTimestampOverflow
		}
		return int64(math.MinInt64) + remainder - minimumRescue, nil
	}
	result := seconds * scale
	return result + remainder, nil
}
