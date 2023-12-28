package klog

import (
	"bytes"
	"fmt"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"strings"
	"time"
)

type LogOpt struct {
	LogLevel     string //trace,debug,info,error
	InfoLogPath  string //full path of info log
	ErrorLogPath string //full path of error log
	MaxAgeDay    int64  //max age of log
	RotateDay    int64  //file rotate day
	RotateSizeM  int64  //file rotate size
}

type KLog struct {
	*LogOpt
	*logrus.Logger
}

type kFormatter struct{}

func (m *kFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var b *bytes.Buffer
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}

	timestamp := entry.Time.Format(TimeStampFormat)

	newLog := fmt.Sprintf("[%s] [%s][%s] %s\n", timestamp, entry.Level, entry.Data[FieldCaller], entry.Message)

	b.WriteString(newLog)
	return b.Bytes(), nil
}

func NewKLog(logOpt *LogOpt) *KLog {
	logger := logrus.New()
	logLev, err := logrus.ParseLevel(logOpt.LogLevel)
	logger.SetLevel(logLev)
	if err != nil {
		logger.SetLevel(logrus.WarnLevel)
	}

	logger.SetOutput(io.Discard)

	instance := &KLog{
		LogOpt: logOpt,
		Logger: logger,
	}

	instance.doInit()

	return instance
}

func (k *KLog) doInit() {
	// print func name, file name and line number
	callerHook := CallerHook{
		Field:  FieldCaller,
		levels: nil,
		short:  true,
	}
	callerHook.SetKipPkg("logrus", "log", "klog")
	k.AddHook(&callerHook)

	//write to file
	if fileHook, err := k.fileHook(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "fileHook error: %v", err)
	} else {
		k.AddHook(fileHook)
	}
}

// local run hook
func (k *KLog) fileHook() (*lfshook.LfsHook, error) {
	var err error
	wrInfo, wrError := io.Discard, io.Discard

	if len(k.InfoLogPath) > 0 {
		infoPath := k.InfoLogPath
		wrInfo, err = rotatelogs.New(
			infoPath+".%Y%m%d",
			rotatelogs.WithLinkName(infoPath),
			rotatelogs.WithMaxAge(time.Duration(k.MaxAgeDay)*24*time.Hour),
			rotatelogs.WithRotationTime(time.Duration(k.RotateDay)*24*time.Hour),
			rotatelogs.WithRotationSize(k.RotateSizeM*1024*1024),
			rotatelogs.WithHandler(rotatelogs.HandlerFunc(func(event rotatelogs.Event) {
				if event.Type() != rotatelogs.FileRotatedEventType {
					return
				} else {
					rotateEvent := event.(*rotatelogs.FileRotatedEvent)

					_, _ = fmt.Fprintf(os.Stderr, "pre:%s, cur:%s", rotateEvent.PreviousFile(), rotateEvent.CurrentFile())
				}
			})),
		)

		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "fileHook wrInfo failed, %s", err)
			return nil, err
		}
	}

	if len(k.ErrorLogPath) > 0 {
		errPath := k.ErrorLogPath
		wrError, err = rotatelogs.New(
			errPath+".%Y%m%d",
			rotatelogs.WithLinkName(errPath),
			rotatelogs.WithMaxAge(time.Duration(k.MaxAgeDay)*24*time.Hour),
			rotatelogs.WithRotationTime(time.Duration(k.RotateDay)*24*time.Hour),
			rotatelogs.WithRotationSize(k.RotateSizeM*1024*1024),
		)

		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "fileHook wrError failed, %s", err)
			return nil, err
		}
	}

	lfsHook := lfshook.NewHook(lfshook.WriterMap{
		logrus.TraceLevel: io.MultiWriter(wrInfo, os.Stdout),
		logrus.DebugLevel: io.MultiWriter(wrInfo, os.Stdout),
		logrus.InfoLevel:  io.MultiWriter(wrInfo, os.Stdout),
		logrus.WarnLevel:  io.MultiWriter(wrInfo, wrError, os.Stdout),
		logrus.ErrorLevel: io.MultiWriter(wrInfo, wrError, os.Stdout),
		logrus.FatalLevel: io.MultiWriter(wrInfo, wrError, os.Stderr),
		logrus.PanicLevel: io.MultiWriter(wrInfo, wrError, os.Stderr),
	},
		&kFormatter{})

	return lfsHook, err
}

func getPackageName(f string) string {
	for {
		lastPeriod := strings.LastIndex(f, ".")
		lastSlash := strings.LastIndex(f, "/")
		if lastPeriod > lastSlash {
			f = f[:lastPeriod]
		} else {
			f = f[lastSlash+1:]
			break
		}
	}

	return f
}
