package main

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

// TestTraceFlushIsBounded: the trace flush at exit runs although the serve
// context has ended by then, and gives up on a collector that never
// answers after traceFlushLimit.
func TestTraceFlushIsBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		start := time.Now()
		err := flushTraces(ctx, func(ctx context.Context) error {
			if ctx.Err() != nil {
				t.Error("the flush got an ended context, so the spans of the shutdown would be lost")
			}
			<-ctx.Done() // a collector that never answers
			return ctx.Err()
		}, traceFlushLimit)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("flush: %v, want the deadline exceeded", err)
		}
		if d := time.Since(start); d != traceFlushLimit {
			t.Fatalf("the flush took %s, want %s", d, traceFlushLimit)
		}
	})
}
