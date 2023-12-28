package stream

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/wsk15046/rsq"
	"github.com/wsk15046/rsq/redisop"
	"sync"
	"time"
)

type producer struct {
	rsq.ILogger

	topic    string
	client   redis.UniversalClient
	maxLen   int64
	sendChan chan *MsgNode

	availableTags map[string]bool
	mutex         *sync.RWMutex
	quit          chan bool
}

func NewProducer(topic string, maxLen int64, cl redis.UniversalClient, l rsq.ILogger) rsq.IMQProducer {

	p := &producer{
		ILogger: l,

		topic:         topic,
		client:        cl,
		maxLen:        maxLen,
		sendChan:      make(chan *MsgNode, 20480),
		mutex:         new(sync.RWMutex),
		availableTags: make(map[string]bool),
		quit:          make(chan bool),
	}

	createTopic(topic, cl)

	return p
}

func (p *producer) Start() {

	p.monitor()

	go func() {
		t := time.NewTimer(0)
		if !t.Stop() {
			<-t.C
		}
		defer t.Stop()

		//batched send message
		batchSize := 32
		m := make(map[string]interface{}, batchSize)
		index := 0

		for {
			select {
			case <-p.quit:
				p.Info("stop producer %s", p.Topic())
				return
			case <-t.C:
				if len(m) > 0 {
					_, err := p.xAdd(m, p.maxLen)
					if err != nil {
						p.Errorf("MQProducer publish failed error: %s", err.Error())
					}

					m = make(map[string]interface{}, batchSize)
					index = 0
				}
			case node := <-p.sendChan:
				cid := fmt.Sprintf("%s-%d-%s", node.Id, index, node.TagId)
				m[cid] = node.Data
				index++

				if len(m) >= batchSize {
					_, err := p.xAdd(m, p.maxLen)
					if err != nil {
						p.Errorf("MQProducer publish failed error: %s", err.Error())
					}

					m = make(map[string]interface{}, batchSize)
					index = 0
				} else {
					if len(m) == 1 {
						if !t.Stop() && len(t.C) > 0 {
							<-t.C
						}
						t.Reset(10 * time.Millisecond)
					}
				}
			}
		}

	}()
}

func (p *producer) monitor() {

	singleMonitor := func() {
		r := redisop.NewRedisHash[ConsumerStat](p.client, p.ILogger)
		statKey := streamStatKey(p.topic)
		h, err := r.GetAll(statKey)
		if err != nil {
			p.Errorf("get stream stat failed %s, err:%s", statKey, err)
			return
		}

		now := time.Now()
		m := make(map[string]bool)

		for k, v := range h {
			if v.UpdateTime.Add(reportAlive * time.Second).Before(now) {
				err = r.Del(statKey, k)
				if err != nil {
					p.Errorf("del hash field failed %s %s %s", statKey, k, err)
					continue
				}
			} else {
				if v.Latency <= latencyTolerance {
					m[v.TagId] = true
				}
			}
		}

		p.mutex.Lock()
		p.availableTags = m
		p.mutex.Unlock()
	}

	singleMonitor()

	go func() {
		for {
			time.Sleep(reportInterval * time.Second)
			singleMonitor()
		}
	}()

}

func (p *producer) available(tagId string) bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	if tagId == tagIdAll && len(p.availableTags) > 0 {
		return true
	}

	if _, ok := p.availableTags[tagId]; ok {
		return true
	}

	p.Errorf("latency too much")

	return false
}

func (p *producer) Topic() string {
	return p.topic
}

func (p *producer) xAdd(data interface{}, maxLen int64) (id string, err error) {
	ctx := context.Background()
	id, err = p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: p.topic,
		MaxLen: maxLen,
		Approx: true,
		ID:     "",
		Values: data,
	}).Result()

	return id, err
}

func (p *producer) Stop() {
	p.quit <- true
}

// Publish targetId
func (p *producer) Publish(Id string, data []byte, tagIds ...string) error {

	if len(tagIds) == 0 {
		tagId := tagIdAll
		if p.available(tagId) {
			node := &MsgNode{
				Id:    Id,
				TagId: tagId,
				Data:  data,
			}

			p.sendChan <- node
		}
	} else {
		for _, tagId := range tagIds {
			if p.available(tagId) {
				node := &MsgNode{
					Id:    Id,
					TagId: tagId,
					Data:  data,
				}

				p.sendChan <- node
			}
		}
	}

	return nil
}
