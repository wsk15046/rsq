package stream

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/wsk15046/rsq"
	"github.com/wsk15046/rsq/redisop"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ConsumerReporter struct {
	rsq.ILogger
	client redis.UniversalClient

	total    int64
	qps      int64
	lastRead string
	mutex    sync.RWMutex
	topic    string
	tag      string
	fullName string
}

func NewConsumerReport(topic, tag, fullName string, cl redis.UniversalClient, l rsq.ILogger) *ConsumerReporter {
	cr := &ConsumerReporter{
		total:    0,
		qps:      0,
		lastRead: "",
		mutex:    sync.RWMutex{},
		topic:    topic,
		tag:      tag,
		fullName: fullName,

		ILogger: l,
		client:  cl,
	}

	return cr
}

func (cr *ConsumerReporter) Update(lastRead string, incr int64) {
	cr.mutex.Lock()
	defer cr.mutex.Unlock()

	cr.lastRead = lastRead
	cr.total += incr
}

func (cr *ConsumerReporter) StartReport() {
	cr.mutex.RLock()
	preTotal := cr.total
	cr.mutex.RUnlock()

	singleReport := func() {
		ctx := context.Background()
		info, e := cr.client.XInfoStream(ctx, cr.topic).Result()
		if e != nil {
			cr.Errorf("MQConsumer:xread:GetStatistic %s", e)
			return
		}

		cr.mutex.RLock()

		latency := int64(-1)
		entryStr := strings.Split(info.LastEntry.ID, "-")
		readStr := strings.Split(cr.lastRead, "-")

		entryId, e1 := strconv.ParseInt(entryStr[0], 10, 64)
		readId, e2 := strconv.ParseInt(readStr[0], 10, 64)
		if e1 == nil && e2 == nil {
			latency = entryId - readId
		}

		curTotal := cr.total

		mqcs := &ConsumerStat{
			Qps:         (curTotal - preTotal) / reportInterval,
			TagId:       cr.tag,
			LastEntryId: info.LastEntry.ID,
			LastReadId:  cr.lastRead,
			Latency:     latency,
			UpdateTime:  time.Unix(time.Now().Unix(), 0),
		}

		cr.mutex.RUnlock()

		r := redisop.NewRedisHash[ConsumerStat](cr.client, cr.ILogger)
		statKey := streamStatKey(cr.topic)
		if err := r.Set(statKey, cr.fullName, mqcs); err != nil {
			cr.Errorf("consumer set failed %s", err.Error())
		}

		preTotal = curTotal
	}

	singleReport()

	go func() {
		for {
			time.Sleep(reportInterval * time.Second)
			singleReport()
		}
	}()
}
