package stream

import (
	"github.com/panjf2000/ants/v2"
	"github.com/wsk15046/rsq"
	"github.com/wsk15046/rsq/test"
	"github.com/wsk15046/rsq/util"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStreamConsumer(t *testing.T) {
	topic := "rsq:consumer_test"
	max := 10000
	rand.Seed(time.Now().Unix())

	total := 0
	process := int32(0)

	//0:广播
	//1: consumer1
	//2: consumer2

	tags := []string{"$", "c1", "c2"}

	var src [10000]string
	for i := 0; i < len(src); i++ {
		tag := tags[rand.Intn(len(tags))]
		src[i] = tag

		if tag == "$" {
			total += 2
		} else {
			total++
		}
	}

	t.Logf("total : %d", total)

	c, l := test.Dependency()

	p := NewProducer(topic, int64(max), c, l)

	consumerHandler := func(id string, data []byte, c rsq.IMQConsumer) {

		index, e := strconv.Atoi(id)
		if e != nil {
			t.Error(e.Error())
			t.Fail()
		}

		orgTag := src[index]

		if orgTag != "$" {
			if orgTag != c.TagId() {
				t.Error("tag different")
				t.Fail()
			}
		}

		atomic.AddInt32(&process, 1)
	}

	c1 := NewConsumer(topic, tags[1], c, l)
	c1.SetHandler(consumerHandler)
	c1.Subscribe()

	c2 := NewConsumer(topic, tags[2], c, l)
	c2.SetHandler(consumerHandler)
	c2.Subscribe()

	p.Start()

	for i := 0; i < len(src); i++ {

		id := strconv.Itoa(i)
		tagId := src[i]
		data := util.RandString(64)

		e := p.Publish(id, []byte(data), tagId)
		if e != nil {
			t.Error(e.Error())
		}
	}
	for {
		cur := atomic.LoadInt32(&process)
		if cur == int32(total) {
			break
		} else {
			time.Sleep(time.Second)
			t.Log(cur)
		}
	}
}

func TestStreamGroup(t *testing.T) {
	topic := "rsq:group_test"

	max := 10000
	length := 100000
	rand.Seed(time.Now().Unix())
	var lock = new(sync.RWMutex)
	var total int64

	antsWorker, err := ants.NewPool(-1)
	if err != nil {
		log.Fatal("antsWorker init failed")
	}

	m := make(map[int]int)

	for i := 0; i < length; i++ {
		m[i] = 0
	}

	c, l := test.Dependency()

	p := NewProducer(topic, int64(max), c, l)

	consumerHandler := func(id string, data []byte, c rsq.IMQConsumer) {

		_ = antsWorker.Submit(func() {
			index, e := strconv.Atoi(id)
			if e != nil {
				t.Error(e.Error())
				t.Fail()
			}

			lock.Lock()
			m[index] = m[index] + 1
			lock.Unlock()

			atomic.AddInt64(&total, 1)
		})
	}

	groups := []string{"mygroup1", "mygroup2", "mygroup3", "mygroup4", "mygroup5"}

	sampleLen := 3

	for i := 0; i < sampleLen; i++ {
		g1 := NewGroup(topic, groups[i], "g1", c, l)
		g1.SetHandler(consumerHandler)
		g1.Subscribe()

		g2 := NewGroup(topic, groups[i], "g2", c, l)
		g2.SetHandler(consumerHandler)
		g2.Subscribe()
	}

	p.Start()

	for i := 0; i < length; i++ {

		id := strconv.Itoa(i)
		//tagId := rand.Intn(1024)
		data := []byte(util.RandString(64))

		//t.Logf("producer message id %s  tag %d --------------", id, tagId)

		e := p.Publish(id, data)
		if e != nil {
			t.Error(e.Error())
		}
	}

	for {
		cur := atomic.LoadInt64(&total)
		if cur == int64(length*sampleLen) {
			break
		} else {
			time.Sleep(1 * time.Second)
			t.Log(cur)
		}
	}

	lock.RLock()
	defer lock.RUnlock()
	for k, v := range m {
		if v != sampleLen {
			t.Error(k, v)
			t.Fail()
		}
	}
}
