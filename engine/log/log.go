package log

import (
	"github.com/astaxie/beego/logs"
	"testing"
)

var log Logger

// SetLevel controls the categories of logs written
func SetLevel(lvl int) {
	log.SetLevel(lvl)
}

func init() {
	beelog := logs.NewLogger(6)
	beelog.SetLogger("console", "")
	beelog.SetLogFuncCallDepth(3)
	log = beelog
}

// Debug prints debug log
func Debug(format string, values ...interface{}) {
	log.Debug(format, values...)
}

// Info prints information log
func Info(format string, values ...interface{}) {
	log.Debug(format, values...)
}

// Notice prints information that should be seen
func Notice(format string, values ...interface{}) {
	log.Notice(format, values...)
}

// Warning prints warnings for user
func Warning(format string, values ...interface{}) {
	log.Warning(format, values...)
}

// Critical prints error informations
func Critical(format string, values ...interface{}) {
	log.Critical(format, values...)
}

// Logger defines the logs levels used by RamSQL engine
type Logger interface {
	SetLevel(lvl int)
	Debug(fmt string, values ...interface{})
	Info(fmt string, values ...interface{})
	Notice(fmt string, values ...interface{})
	Warning(fmt string, values ...interface{})
	Critical(fmt string, values ...interface{})
}

// TestLogger uses *testing.T as a backend for RamSQL logs
type TestLogger struct {
	t *testing.T
}

// SetLevel is not handled by TestLogger
func (l TestLogger) SetLevel(lvl int) {
}

// Debug uses testing.T.Logf
func (l TestLogger) Debug(fmt string, values ...interface{}) {
	l.t.Logf(fmt, values...)
}

// Info uses testing.T.Logf
func (l TestLogger) Info(fmt string, values ...interface{}) {
	l.t.Logf(fmt, values...)
}

// Notice uses testing.T.Logf
func (l TestLogger) Notice(fmt string, values ...interface{}) {
	l.t.Logf(fmt, values...)
}

// Warning uses testing.T.Logf
func (l TestLogger) Warning(fmt string, values ...interface{}) {
	l.t.Logf(fmt, values...)
}

// Critical uses testing.T.Logf
func (l TestLogger) Critical(fmt string, values ...interface{}) {
	l.t.Logf(fmt, values...)
}

// UseTestLogger should be used only by unit tests
func UseTestLogger(t *testing.T) {
	logger := TestLogger{
		t: t,
	}

	log = logger
}
