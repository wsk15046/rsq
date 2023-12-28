package stream

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/wsk15046/rsq"
	"time"
)

type consumer struct {
	rsq.ILogger

	topic   string
	name    string
	tagId   string
	client  redis.UniversalClient
	handler rsq.ConsumerHandler
	quit    chan bool

	cr *ConsumerReporter
}

func NewConsumer(topic, name string, cl redis.UniversalClient, l rsq.ILogger) rsq.IMQConsumer {
	c := &consumer{
		ILogger: l,

		name:   name,
		topic:  topic,
		tagId:  name,
		client: cl,
		quit:   make(chan bool),
	}

	c.cr = NewConsumerReport(topic, c.tagId, c.FullName(), cl, l)

	createTopic(topic, cl)

	return c
}

func (c *consumer) SetHandler(h rsq.ConsumerHandler) {
	c.handler = h
}

func (c *consumer) Topic() string {
	return c.topic
}

func (c *consumer) FullName() string {
	return fmt.Sprintf("%s@%s", consumerPrefix, c.name)
}

func (c *consumer) TagId() string {
	return c.tagId
}

func (c *consumer) Subscribe() {
	c.cr.StartReport()
	go c.xRead()
}

func (c *consumer) Stop() {
	c.quit <- true
}

func (c *consumer) xRead() {
	ctx := context.Background()

	count := int64(0)
	id := "$"

	for {
		select {
		case <-c.quit:
			c.Errorf("consumer quit %s", c.topic)
			return
		default:
			data, errRead := c.client.XRead(ctx, &redis.XReadArgs{
				Streams: []string{c.topic, id},
				Count:   10000,
				Block:   0,
			}).Result()

			if data != nil && len(data) > 0 {
				var ids []string
				for _, result := range data {
					for _, message := range result.Messages {
						if c.handler != nil {

							sortMsg := preTreatMsgs(message.Values, c.ILogger)

							for _, node := range sortMsg {
								if node == nil {
									continue
								}

								pid := node.Id
								pData := node.Data
								tagId := node.TagId

								// not mine nor broadcast
								if tagId != tagIdAll && c.tagId != tagIdAll && c.tagId != tagId {
									continue
								}

								//Closure trap, must pass new variables into the closure function
								if pid != msgIdCreateTopic {
									c.handler(pid, pData, c)
								}
							}

							ids = append(ids, message.ID)
							id = message.ID
							count += int64(len(sortMsg))
						}
					}
				}

				if l := len(ids); l > 0 {
					c.cr.Update(ids[l-1], count)
					count = 0
				}

			} else {
				if errRead != redis.Nil {
					c.Errorf("MQConsumer:xRead:err_read %s", errRead.Error())
					time.Sleep(time.Second)
				}
			}
		}
	}
}

func (c *consumer) xDel(ctx context.Context, ids []string) (cnt int64, err error) {
	return c.client.XDel(ctx, c.topic, ids...).Result()
}

func (c *consumer) xLen(ctx context.Context) (cnt int64, err error) {
	return c.client.XLen(ctx, c.topic).Result()
}

func (c *consumer) xRange(ctx context.Context, start, stop string) (msgs []redis.XMessage, err error) {
	return c.client.XRange(ctx, c.topic, start, stop).Result()
}

func (c *consumer) xRevarange(ctx context.Context, start, stop string) (msgs []redis.XMessage, err error) {
	return c.client.XRevRange(ctx, c.topic, start, stop).Result()
}
