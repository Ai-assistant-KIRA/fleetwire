package uplink

import (
	"io"
	"log/slog"
	"os"
)

func discard() *slog.Logger {
	if os.Getenv("FLEETWIRE_TEST_LOG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
