package rsq

type ConsumerHandler func(id string, data []byte, h IMQConsumer)

type IMQProducer interface {
	Topic() string
	Start()
	Publish(id string, data []byte, tagId ...string) (err error)
	Stop()
}

type IMQConsumer interface {
	Topic() string
	FullName() string
	TagId() string
	Subscribe()
	SetHandler(h ConsumerHandler)
	Stop()
}

type ILogger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Printf(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Warningf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
	Panicf(format string, args ...interface{})

	Debug(args ...interface{})
	Info(args ...interface{})
	Print(args ...interface{})
	Warn(args ...interface{})
	Warning(args ...interface{})
	Error(args ...interface{})
	Fatal(args ...interface{})
	Panic(args ...interface{})
}
