package stream

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/wsk15046/rsq"
	"time"
)

type Group struct {
	rsq.ILogger

	topic   string
	group   string
	name    string
	tagId   string
	client  redis.UniversalClient
	handler rsq.ConsumerHandler
	quit    chan bool

	cr *ConsumerReporter
}

//In the case of the same topic
//if the group name is the same, messages will be randomly distributed to consumers within the same group.
//If the group name is different, messages will be broadcasted to different groups.

func NewGroup(topic, group, name string, cl redis.UniversalClient, l rsq.ILogger) *Group {

	g := &Group{
		ILogger: l,

		topic:  topic,
		group:  group,
		name:   name,
		tagId:  group,
		client: cl,
		quit:   make(chan bool),
	}

	g.cr = NewConsumerReport(topic, g.tagId, g.FullName(), cl, l)

	createTopic(topic, cl)

	if _, e := g.xGroupCreate(); e != nil {
		l.Warnf("create group failed [ %s ]", e.Error())
	}

	return g
}

func (g *Group) SetHandler(h rsq.ConsumerHandler) {
	g.handler = h
}

func (g *Group) TagId() string {
	return g.tagId
}

func (g *Group) FullName() string {
	return fmt.Sprintf("%s@%s@%s", groupPrefix, g.group, g.name)
}

func (g *Group) Topic() string {
	return g.topic
}

func (g *Group) Subscribe() {
	g.cr.StartReport()
	go g.xReadGroup()
}

func (g *Group) Stop() {
	g.quit <- true
}

func (g *Group) xGroupCreate() (ret string, err error) {
	var ctx = context.Background()
	return g.client.XGroupCreate(ctx, g.topic, g.group, "0").Result()
}

// During startup, start by checking and reading messages from the beginning,
// including those that were previously read but not confirmed as consumed by the current consumer.
// Once processing is completed, subsequent reads will retrieve only the latest data.
func (g *Group) xReadGroup() {

	ctx := context.Background()
	lastId := ">" // consume from lastdeliveredID
	checkBacklog := true

	// in millisecond
	count := int64(0)

	for {
		select {
		case <-g.quit:
			g.Errorf("consumer quit %s", g.topic)
			return
		default:

			var id string
			if checkBacklog {
				id = lastId
			} else {
				id = ">"
			}

			data, errRead := g.client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    g.group,
				Consumer: g.name,
				Streams:  []string{g.topic, id},
				Count:    10000,
				Block:    0,
				NoAck:    false,
			}).Result()

			if data != nil && len(data) > 0 {

				// Indicate that the previously unacknowledged messages have been processed,
				// and begin handling new messages. Change lastId to ">".
				if len(data[0].Messages) == 0 {
					checkBacklog = false
				}

				var ids []string

				for _, result := range data {
					for _, message := range result.Messages {
						if g.handler != nil {

							sortMsg := preTreatMsgs(message.Values, g.ILogger)

							for _, node := range sortMsg {
								if node == nil {
									continue
								}

								pid := node.Id
								pData := node.Data
								tagId := node.TagId

								// not mine nor broadcast
								if tagId != tagIdAll && g.tagId != tagIdAll && g.tagId != tagId {
									continue
								}

								//Closure trap, must pass new variables into the closure function
								if pid != msgIdCreateTopic {
									g.handler(pid, pData, g)
								}
							}

							ids = append(ids, message.ID)
							id = message.ID
							count += int64(len(sortMsg))
						}
					}
				}

				if l := len(ids); l > 0 {

					g.cr.Update(ids[l-1], count)

					count = 0

					if _, errAck := g.xAck(ctx, ids...); errAck != nil {
						g.Errorf("MQGroup:xReadGroup:err_ack: %s, topic: %s, group: %s, name: %s",
							errAck, g.topic, g.group, g.name)
						time.Sleep(time.Second)
					}
				}

			} else {
				if errRead != redis.Nil {
					g.Errorf("MQGroup:xReadGroup:err_read: %s, topic: %s, group: %s, name: %s",
						errRead, g.topic, g.group, g.name)
					time.Sleep(time.Second)
				}
			}
		}
	}
}

func (g *Group) xAck(ctx context.Context, ids ...string) (ret int64, err error) {
	return g.client.XAck(ctx, g.topic, g.group, ids...).Result()
}

func (g *Group) xClaim(ctx context.Context, monitoringConsumerName string, toBeClaimed []string) (err error) {
	_, err = g.client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   g.topic,
		Group:    g.group,
		Consumer: monitoringConsumerName,
		MinIdle:  time.Duration(60) * time.Second,
		Messages: toBeClaimed,
	}).Result()

	return nil
}

func (g *Group) xPending(ctx context.Context, cnt int64) (*redis.XPending, error) {
	return g.client.XPending(ctx, g.topic, g.group).Result()
}

func (g *Group) xInfoGroup(ctx context.Context) (infos []redis.XInfoGroup, err error) {
	return g.client.XInfoGroups(ctx, g.topic).Result()
}

func (g *Group) xInfoStream(ctx context.Context) (info *redis.XInfoStream, err error) {
	return g.client.XInfoStream(ctx, g.topic).Result()
}
