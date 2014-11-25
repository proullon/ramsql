package engine

import (
	"github.com/astaxie/beego"
)

var log = beego.BeeLogger

func initLog() {
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
