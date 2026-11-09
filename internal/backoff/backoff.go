// Package backoff implements exponential backoff with full jitter.
//
// Full jitter (a uniform random delay between zero and the exponential cap)
// spreads reconnecting clients over the whole window. After a broker restart,
// thousands of devices would otherwise retry in synchronized waves.
package backoff

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
)

// Backoff computes retry delays. The zero value is not usable; set Base and Max.
type Backoff struct {
	// Base is the cap for the first retry.
	Base time.Duration
	// Max is the largest cap. Delays never exceed it.
	Max time.Duration
	// Int64N returns a uniform random number in [0, n). Nil means math/rand/v2.
	// Tests inject a fixed source.
	Int64N func(n int64) int64
}

// Delay returns the delay before retry number attempt (0 for the first retry):
// a uniform random value in [0, min(Max, Base*2^attempt)].
func (b Backoff) Delay(attempt int) time.Duration {
	limit := b.limit(attempt)
	if limit <= 0 {
		return 0
	}
	n := b.Int64N
	if n == nil {
		n = rand.Int64N
	}
	// +1 makes the cap itself reachable.
	return time.Duration(n(int64(limit) + 1))
}

// limit is min(Max, Base*2^attempt) without overflowing.
func (b Backoff) limit(attempt int) time.Duration {
	if b.Base <= 0 || b.Max <= 0 {
		return 0
	}
	if attempt < 0 {
		attempt = 0
	}
	d := b.Base
	for i := 0; i < attempt; i++ {
		if d >= b.Max/2 {
			return b.Max
		}
		d *= 2
	}
	return min(d, b.Max)
}

// ErrPermanent marks an error that must not be retried. Wrap it with
// Permanent; Retry returns the wrapped error unchanged.
var ErrPermanent = errors.New("permanent error")

type permanentError struct{ err error }

func (p permanentError) Error() string        { return p.err.Error() }
func (p permanentError) Unwrap() []error      { return []error{p.err, ErrPermanent} }
func (p permanentError) Is(target error) bool { return target == ErrPermanent }

// Permanent wraps err so that Retry stops and returns it.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err: err}
}

// RetryOptions tune Retry.
type RetryOptions struct {
	// MaxAttempts stops after this many calls. Zero means no limit.
	MaxAttempts int
	// Retryable reports whether an error is worth another try. Nil means every
	// error that is not marked Permanent is retryable.
	Retryable func(error) bool
	// OnRetry is called before each wait, for logging and metrics.
	OnRetry func(attempt int, err error, delay time.Duration)
}

// Retry calls fn until it returns nil, returns a non-retryable error, the
// attempt limit is reached, or ctx is done. It returns the last error from fn,
// or ctx.Err() wrapped with the last error when the context ends first.
func Retry(ctx context.Context, b Backoff, opts RetryOptions, fn func(ctx context.Context) error) error {
	for attempt := 0; ; attempt++ {
		err := fn(ctx)
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrPermanent) {
			var p permanentError
			if errors.As(err, &p) {
				return p.err
			}
			return err
		}
		if opts.Retryable != nil && !opts.Retryable(err) {
			return err
		}
		if opts.MaxAttempts > 0 && attempt+1 >= opts.MaxAttempts {
			return fmt.Errorf("giving up after %d attempts: %w", attempt+1, err)
		}
		delay := b.Delay(attempt)
		if opts.OnRetry != nil {
			opts.OnRetry(attempt, err, delay)
		}
		if sleepErr := Sleep(ctx, delay); sleepErr != nil {
			return fmt.Errorf("%w (last error: %w)", sleepErr, err)
		}
	}
}

// Sleep waits for d or until ctx is done, and returns ctx.Err() in that case.
func Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
