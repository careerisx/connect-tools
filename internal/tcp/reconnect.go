package tcp

import (
	"fmt"
	"math"
	"time"
)

// Backoff controls cancellable TCP client reconnect delays. Zero values select
// Initial 250ms, Max 5s, and Multiplier 2.
type Backoff struct {
	Initial    time.Duration
	Max        time.Duration
	Multiplier float64
}

func (backoff Backoff) validate() error {
	if backoff.Initial < 0 || backoff.Max < 0 || backoff.Multiplier < 0 || math.IsNaN(backoff.Multiplier) || math.IsInf(backoff.Multiplier, 0) {
		return fmt.Errorf("%w: invalid reconnect backoff", ErrInvalidConfig)
	}
	if backoff.Initial > 0 && backoff.Max > 0 && backoff.Max < backoff.Initial {
		return fmt.Errorf("%w: reconnect maximum is less than initial delay", ErrInvalidConfig)
	}
	if backoff.Multiplier > 0 && backoff.Multiplier < 1 {
		return fmt.Errorf("%w: reconnect multiplier must be at least one", ErrInvalidConfig)
	}
	return nil
}

func (backoff Backoff) normalized() Backoff {
	if backoff.Initial <= 0 {
		backoff.Initial = 250 * time.Millisecond
	}
	if backoff.Max <= 0 {
		backoff.Max = 5 * time.Second
	}
	if backoff.Max < backoff.Initial {
		backoff.Max = backoff.Initial
	}
	if backoff.Multiplier < 1 {
		backoff.Multiplier = 2
	}
	return backoff
}

// Delay returns the bounded delay for a zero-based reconnect attempt.
func (backoff Backoff) Delay(attempt int) time.Duration {
	backoff = backoff.normalized()
	if attempt <= 0 {
		return backoff.Initial
	}
	delay := float64(backoff.Initial) * math.Pow(backoff.Multiplier, float64(attempt))
	if math.IsInf(delay, 1) || delay >= float64(backoff.Max) {
		return backoff.Max
	}
	return time.Duration(delay)
}
