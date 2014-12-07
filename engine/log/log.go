package log

import (
	"github.com/astaxie/beego/logs"
)

var log *logs.BeeLogger

func SetLevel(lvl int) {
	log.SetLevel(lvl)
}

func init() {
	log = logs.NewLogger(10000)
	log.SetLogger("console", "")
	log.SetLogFuncCallDepth(3)
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
