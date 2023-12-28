package klog

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"runtime"
	"strings"
)

const (
	TimeStampFormat        = "2006-01-02 15:04:05.265"
	FieldCaller            = "caller"
	maximumCallerDepth int = 25
	minimumCallerDepth int = 4
)

type CallerHook struct {
	Field  string
	KipPkg string
	levels []logrus.Level
	short  bool
}

// Levels implement levels

func (hook *CallerHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

// Fire implement fire
func (hook *CallerHook) Fire(entry *logrus.Entry) error {
	if true {
		entry.Caller = hook.getCaller()
		fileName := entry.Caller.File

		//shot file name
		if hook.short {
			for i := len(entry.Caller.File) - 1; i > 0; i-- {
				if entry.Caller.File[i] == '/' {
					fileName = entry.Caller.File[i+1:]
					break
				}
			}
		}

		fileVal := fmt.Sprintf("%s:%d", fileName, entry.Caller.Line)
		entry.Data[hook.Field] = fileVal
	}
	return nil
}

func (hook *CallerHook) getCaller() *runtime.Frame {
	// Restrict the lookback frames to avoid runaway lookups
	pcs := make([]uintptr, maximumCallerDepth)
	depth := runtime.Callers(minimumCallerDepth, pcs)
	frames := runtime.CallersFrames(pcs[:depth])
	for f, again := frames.Next(); again; f, again = frames.Next() {
		pkg := getPackageName(f.Function)
		// If the caller isn't part of this package, we're done
		if !strings.Contains(hook.GetKipPkg(), pkg) {
			return &f
		}
	}
	// if we got here, we failed to find the caller's context
	return nil
}

func (hook *CallerHook) SetKipPkg(args ...string) {
	hook.KipPkg = strings.Join(args, ",")
}

func (hook *CallerHook) GetKipPkg() string {
	if hook.KipPkg == "" {
		return "logrus,logs"
	}
	return hook.KipPkg
}
