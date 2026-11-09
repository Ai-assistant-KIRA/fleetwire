package backoff

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func maxSource(n int64) int64 { return n - 1 }

func TestDelayGrowsAndCaps(t *testing.T) {
	b := Backoff{Base: time.Second, Max: 60 * time.Second, Int64N: maxSource}
	want := []int64{1, 2, 4, 8, 16, 32, 60, 60}
	for attempt, w := range want {
		if got := b.Delay(attempt); got != time.Duration(w)*time.Second {
			t.Errorf("Delay(%d) = %v, want %ds", attempt, got, w)
		}
	}
	// Huge attempt numbers must not overflow into negative or tiny delays.
	if got := b.Delay(10_000); got != 60*time.Second {
		t.Errorf("Delay(10000) = %v, want cap", got)
	}
}

func TestDelayIsWithinBounds(t *testing.T) {
	b := Backoff{Base: 100 * time.Millisecond, Max: 5 * time.Second}
	for attempt := 0; attempt < 20; attempt++ {
		limit := b.limit(attempt)
		for i := 0; i < 200; i++ {
			if d := b.Delay(attempt); d < 0 || d > limit {
				t.Fatalf("Delay(%d) = %v outside [0, %v]", attempt, d, limit)
			}
		}
	}
}

func TestDelayZeroConfig(t *testing.T) {
	if d := (Backoff{}).Delay(3); d != 0 {
		t.Fatalf("zero Backoff delay = %v, want 0", d)
	}
}

func TestRetryStopsOnSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		start := time.Now()
		err := Retry(t.Context(), Backoff{Base: time.Second, Max: time.Minute, Int64N: maxSource}, RetryOptions{},
			func(context.Context) error {
				calls++
				if calls < 4 {
					return errors.New("transient")
				}
				return nil
			})
		if err != nil {
			t.Fatalf("Retry() = %v", err)
		}
		if calls != 4 {
			t.Fatalf("calls = %d, want 4", calls)
		}
		// Waits of 1 s + 2 s + 4 s with the max source.
		if elapsed := time.Since(start); elapsed != 7*time.Second {
			t.Fatalf("elapsed = %v, want 7s", elapsed)
		}
	})
}

func TestRetryPermanent(t *testing.T) {
	base := errors.New("bad input")
	calls := 0
	err := Retry(t.Context(), Backoff{Base: time.Second, Max: time.Minute}, RetryOptions{},
		func(context.Context) error { calls++; return Permanent(base) })
	if !errors.Is(err, base) || errors.Is(err, ErrPermanent) || calls != 1 {
		t.Fatalf("Retry() = %v after %d calls; want the unwrapped base error after 1 call", err, calls)
	}
}

func TestRetryClassifier(t *testing.T) {
	fatal := errors.New("fatal")
	calls := 0
	err := Retry(t.Context(), Backoff{Base: time.Millisecond, Max: time.Millisecond},
		RetryOptions{Retryable: func(err error) bool { return !errors.Is(err, fatal) }},
		func(context.Context) error { calls++; return fatal })
	if !errors.Is(err, fatal) || calls != 1 {
		t.Fatalf("Retry() = %v after %d calls", err, calls)
	}
}

func TestRetryMaxAttempts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		err := Retry(t.Context(), Backoff{Base: time.Second, Max: time.Second},
			RetryOptions{MaxAttempts: 3}, func(context.Context) error { calls++; return errors.New("x") })
		if err == nil || calls != 3 {
			t.Fatalf("Retry() = %v after %d calls, want error after 3", err, calls)
		}
	})
}

func TestRetryContextCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		last := errors.New("broker down")
		err := Retry(ctx, Backoff{Base: time.Hour, Max: time.Hour, Int64N: maxSource}, RetryOptions{},
			func(context.Context) error { return last })
		if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, last) {
			t.Fatalf("Retry() = %v, want deadline exceeded wrapping the last error", err)
		}
	})
}
