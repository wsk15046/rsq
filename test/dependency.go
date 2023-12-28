package test

import (
	"github.com/go-redis/redis/v8"
	"github.com/wsk15046/rsq"
	"github.com/wsk15046/rsq/klog"
	"sync"
)

var (
	c    *redis.ClusterClient
	l    rsq.ILogger
	once sync.Once
)

func Dependency() (*redis.ClusterClient, rsq.ILogger) {

	once.Do(func() {
		c = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: []string{"192.168.12.108:7361", "192.168.12.108:7362", "192.168.12.108:7363",
				"192.168.12.108:7364", "192.168.12.108:7365", "192.168.12.108:7366"},
			Password: "123456",
		})
		l = klog.NewKLog(&klog.LogOpt{
			LogLevel:    "info",
			InfoLogPath: "./../logs",
			MaxAgeDay:   180,
			RotateDay:   1,
			RotateSizeM: 1000,
		})
	})

	return c, l
}
