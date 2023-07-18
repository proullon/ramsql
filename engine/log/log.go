package log

import (
	"context"
	"fmt"
	"golang.org/x/exp/slog"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

var (
	logger *slog.Logger
	level  *slog.LevelVar
)

func init() {
	replace := func(groups []string, a slog.Attr) slog.Attr {
		// Remove the directory from the source's filename.
		if a.Key == slog.SourceKey {
			source := a.Value.Any().(*slog.Source)
			source.File = filepath.Base(source.File)
		}
		return a
	}
	level = new(slog.LevelVar)
	logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{AddSource: true, Level: level, ReplaceAttr: replace}))
	slog.SetDefault(logger)
	SetLevel(WarningLevel)
}

// Level of logging trigger
type Level int

// Available logging levels
const (
	DebugLevel Level = iota
	InfoLevel
	WarningLevel
	ErrorLevel
)

// Logger defines the logs levels used by RamSQL engine
type Logger interface {
	Logf(fmt string, values ...interface{})
}

// SetLevel controls the categories of logs written
func SetLevel(lvl Level) {
	switch lvl {
	case DebugLevel:
		level.Set(slog.LevelDebug)
	case WarningLevel:
		level.Set(slog.LevelWarn)
	case ErrorLevel:
		level.Set(slog.LevelError)
	default:
		level.Set(slog.LevelInfo)
	}
}

func Debug(format string, args ...any) {
	if !logger.Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	var pcs [1]uintptr
	runtime.Callers(2, pcs[:]) // skip [Callers, Infof]
	r := slog.NewRecord(time.Now(), slog.LevelDebug, fmt.Sprintf(format, args...), pcs[0])
	_ = logger.Handler().Handle(context.Background(), r)
}

func Info(format string, args ...any) {
	if !logger.Enabled(context.Background(), slog.LevelInfo) {
		return
	}
	var pcs [1]uintptr
	runtime.Callers(2, pcs[:]) // skip [Callers, Infof]
	r := slog.NewRecord(time.Now(), slog.LevelInfo, fmt.Sprintf(format, args...), pcs[0])
	_ = logger.Handler().Handle(context.Background(), r)
}

func Warn(format string, args ...any) {
	if !logger.Enabled(context.Background(), slog.LevelWarn) {
		return
	}
	var pcs [1]uintptr
	runtime.Callers(2, pcs[:]) // skip [Callers, Infof]
	r := slog.NewRecord(time.Now(), slog.LevelWarn, fmt.Sprintf(format, args...), pcs[0])
	_ = logger.Handler().Handle(context.Background(), r)
}

func Error(format string, args ...any) {
	if !logger.Enabled(context.Background(), slog.LevelError) {
		return
	}
	var pcs [1]uintptr
	runtime.Callers(2, pcs[:]) // skip [Callers, Infof]
	r := slog.NewRecord(time.Now(), slog.LevelError, fmt.Sprintf(format, args...), pcs[0])
	_ = logger.Handler().Handle(context.Background(), r)
}
