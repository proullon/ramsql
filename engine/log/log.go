package log

import (
	// TODO: add a file logger
	"log"
	"testing"
)

// Level of logging trigger
type Level int

// Available logging levels
const (
	DebugLevel Level = iota
	InfoLevel
	NoticeLevel
	WarningLevel
	CriticalLevel
)

var (
	logger Logger
	level  Level
)

// Logger defines the logs levels used by RamSQL engine
type Logger interface {
	Logf(fmt string, values ...interface{})
}

// SetLevel controls the categories of logs written
func SetLevel(lvl Level) {
	level = lvl
}

func init() {
	level = WarningLevel
	logger = BaseLogger{}
}

// Debug prints debug log
func Debug(format string, values ...interface{}) {
	if level <= DebugLevel {
		logger.Logf("[DEBUG]    "+format, values...)
	}
}

// Info prints information log
func Info(format string, values ...interface{}) {
	if level <= InfoLevel {
		logger.Logf("[INFO]     "+format, values...)
	}
}

// Notice prints information that should be seen
func Notice(format string, values ...interface{}) {
	if level <= NoticeLevel {
		logger.Logf("[NOTICE]   "+format, values...)
	}
}

// Warning prints warnings for user
func Warning(format string, values ...interface{}) {
	if level <= WarningLevel {
		logger.Logf("[WARNING]  "+format, values...)
	}
}

// Critical prints error informations
func Critical(format string, values ...interface{}) {
	logger.Logf("[CRITICAL] "+format, values...)
}

// BaseLogger logs on stdout
type BaseLogger struct {
}

// Logf logs on stdout
func (l BaseLogger) Logf(fmt string, values ...interface{}) {
	log.Printf(fmt, values...)
}

// TestLogger uses *testing.T as a backend for RamSQL logs
type TestLogger struct {
	t *testing.T
}

// Logf logs in testing log buffer
func (l TestLogger) Logf(fmt string, values ...interface{}) {
	l.t.Logf(fmt, values...)
}

// UseTestLogger should be used only by unit tests
func UseTestLogger(t *testing.T) {
	logger = t
}
