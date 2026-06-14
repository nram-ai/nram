package logging

import (
	"context"
	"io"
	"log/slog"
	"strings"
)

// stdBridge adapts the standard library log package's io.Writer output into the
// structured logger, so any remaining log.Print output is captured by the same
// pipeline. Lines are emitted at info with the "stdlog" component. This is a
// safety net; first-class call sites use slog / Named directly.
type stdBridge struct{}

func (stdBridge) Write(p []byte) (int, error) {
	msg := strings.TrimRight(string(p), "\n")
	if msg != "" {
		slog.Default().LogAttrs(context.Background(), slog.LevelInfo, msg,
			slog.String(ComponentKey, "stdlog"))
	}
	return len(p), nil
}

// StdBridge returns an io.Writer suitable for log.SetOutput that forwards
// stdlib log output through the default slog handler.
func StdBridge() io.Writer { return stdBridge{} }
