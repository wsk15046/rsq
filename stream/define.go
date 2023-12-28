package stream

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/wsk15046/rsq"
	"strconv"
	"strings"
	"time"
)

type MsgNode struct {
	Id    string
	TagId string
	Data  []byte
}

type ConsumerStat struct {
	Qps         int64
	TagId       string
	LastEntryId string
	LastReadId  string
	Latency     int64
	UpdateTime  time.Time
}

const reportInterval = 1 //s
const reportAlive = 60   //s

const blockRead = 1000 //ms

const latencyTolerance = 5000 //ms

const msgIdCreateTopic = "createTopic"
const tagIdAll = "$"

const defaultStreamLen = 10000

const keyStreamStat = "%s_stat"

const consumerPrefix = "consumer"
const groupPrefix = "group"

func preTreatMsgs(msgs map[string]interface{}, l rsq.ILogger) (sortMsg []*MsgNode) {

	sortMsg = make([]*MsgNode, len(msgs))

	for rawId, msg := range msgs {
		vals := strings.Split(rawId, "-")
		sid := vals[0]
		if sid == msgIdCreateTopic {
			continue
		}

		if len(vals) < 3 {
			continue
		}

		index, e := strconv.ParseInt(vals[1], 10, 32)
		if e != nil {
			l.Errorf("invalid message [%s]", rawId)
			continue
		}

		data := []byte(fmt.Sprintf("%v", msg.(interface{})))

		sortMsg[index] = &MsgNode{
			Id:    sid,
			TagId: vals[2],
			Data:  data,
		}
	}

	return sortMsg
}

func createTopic(topic string, client redis.UniversalClient) {
	//To add a message to a queue using XADD, if the specified queue does not exist, you can create a stream.

	node := &MsgNode{
		Id:    msgIdCreateTopic,
		TagId: tagIdAll,
		Data:  []byte(time.Now().String()),
	}

	cid := fmt.Sprintf("%s-%d-%s", node.Id, 0, node.TagId)
	m := make(map[string]interface{})
	m[cid] = node.Data

	ctx := context.Background()

	_, _ = client.XAdd(ctx, &redis.XAddArgs{
		Stream: topic,
		ID:     "",
		MaxLen: defaultStreamLen,
		Approx: true,
		Values: m,
	}).Result()
}

func streamStatKey(topic string) string {
	return fmt.Sprintf(keyStreamStat, topic)
}
