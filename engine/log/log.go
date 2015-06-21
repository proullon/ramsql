package log

import (
	"github.com/astaxie/beego/logs"
	"testing"
)

var log Logger

func SetLevel(lvl int) {
	log.SetLevel(lvl)
}

func init() {
	beelog := logs.NewLogger(0)
	beelog.SetLogger("console", "")
	beelog.SetLogFuncCallDepth(3)
	log = beelog
}

func Debug(format string, values ...interface{}) {
	log.Debug(format, values...)
}

func Info(format string, values ...interface{}) {
	log.Debug(format, values...)
}

func Notice(format string, values ...interface{}) {
	log.Notice(format, values...)
}

func Warning(format string, values ...interface{}) {
	log.Warning(format, values...)
}

func Critical(format string, values ...interface{}) {
	log.Critical(format, values...)
}

type Logger interface {
	SetLevel(lvl int)
	Debug(fmt string, values ...interface{})
	Info(fmt string, values ...interface{})
	Notice(fmt string, values ...interface{})
	Warning(fmt string, values ...interface{})
	Critical(fmt string, values ...interface{})
}

type TestLogger struct {
	t *testing.T
}

func (l TestLogger) SetLevel(lvl int) {
}

func (l TestLogger) Debug(fmt string, values ...interface{}) {
	l.t.Logf(fmt, values...)
}

func (l TestLogger) Info(fmt string, values ...interface{}) {
	l.t.Logf(fmt, values...)
}

func (l TestLogger) Notice(fmt string, values ...interface{}) {
	l.t.Logf(fmt, values...)
}

func (l TestLogger) Warning(fmt string, values ...interface{}) {
	l.t.Logf(fmt, values...)
}

func (l TestLogger) Critical(fmt string, values ...interface{}) {
	l.t.Logf(fmt, values...)
}

func UseTestLogger(t *testing.T) {
	logger := TestLogger{
		t: t,
	}

	log = logger
}
