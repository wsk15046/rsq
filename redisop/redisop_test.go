package redisop

import (
	"fmt"
	"github.com/wsk15046/rsq/test"
	"github.com/wsk15046/rsq/util"
	"math/rand"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"
)

type MyStruct struct {
	A int
	B string
	C time.Time
}

func (s *MyStruct) EqualTo(o *MyStruct) bool {
	return s.A == o.A && s.B == o.B && s.C.Equal(o.C)
}

func TestZSetRedis(t *testing.T) {
	c, l := test.Dependency()

	rand.Seed(time.Now().Unix())

	rr := NewRedisZSet[string](c, l)

	var group = new(sync.WaitGroup)

	const zSetCount = 10
	const opCount = 10
	const trimLen = 3

	var keys []string

	for i := 0; i < zSetCount; i++ {

		group.Add(1)

		go func() {
			defer group.Done()

			key := fmt.Sprintf("base:zsettest:%s", util.RandString(rand.Intn(20)+1))

			keys = append(keys, key)

			var subGroup = new(sync.WaitGroup)

			for j := 0; j < opCount; j++ {

				subGroup.Add(1)

				go func(subGroup *sync.WaitGroup, key string, j int) {
					defer subGroup.Done()

					//s := &MyStruct{
					//	A: rand.Int(),
					//	B: util.RandString(rand.Intn(1024)),
					//	C: time.Unix(time.Now().Unix(), 0),
					//}

					raw := strconv.Itoa(j)
					s := &raw

					err := rr.ZAdd(key, s, float64(j))
					if err != nil {
						t.Error(err)
					}

					time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)

					err = rr.ZTrim(key, trimLen, false)
					if err != nil {
						t.Error(err)
					}
				}(subGroup, key, j)
			}

			subGroup.Wait()

		}()

		group.Wait()
	}

	for index, k := range keys {
		cnt, err := rr.ZCard(k)
		if err != nil {
			t.Error(err.Error())
		}

		if cnt != int64(trimLen) {
			t.Errorf("length not equal,index:%d,key:%s,cnt:%d", index, k, cnt)
		}

		raw := strconv.Itoa(index)

		cnt, err = rr.ZRem(k, &raw)
		if err != nil {
			t.Error(err.Error())
		}
	}
}

func TestStringRedis(t *testing.T) {
	c, l := test.Dependency()

	rand.Seed(time.Now().Unix())

	rh := NewRedisString[MyStruct](c, l)

	var group = new(sync.WaitGroup)

	for i := 0; i < 100; i++ {

		group.Add(1)

		go func() {
			defer group.Done()

			key := fmt.Sprintf("base:stringtest:%s", util.RandString(rand.Intn(20)+1))

			s := &MyStruct{
				A: rand.Int(),
				B: util.RandString(rand.Intn(1024)),
				C: time.Unix(time.Now().Unix(), 0),
			}

			err := rh.Set(key, s, 0)
			if err != nil {
				t.Error(err)
			}

			o, err := rh.Get(key)
			if err != nil {
				t.Error(err)
			}

			if !reflect.DeepEqual(s, o) {
				t.Error("not equal")
			}

			err = rh.Del(key)
			if err != nil {
				t.Error(err)
			}
		}()
	}

	group.Wait()
}

func TestHashStruct(t *testing.T) {

	c, l := test.Dependency()

	key := "base:hashtest"

	rh := NewRedisHash[MyStruct](c, l)

	if err := rh.DelAll(key); err != nil {
		t.Error(err)
	}

	var group = new(sync.WaitGroup)

	for i := 0; i < 100; i++ {

		k := i

		group.Add(1)

		go func(index int) {

			defer group.Done()

			fKey := fmt.Sprintf("192.168.1.80_%d", index)

			s := &MyStruct{
				A: rand.Int(),
				B: util.RandString(rand.Intn(1024)),
				C: time.Unix(time.Now().Unix(), 0),
			}

			err := rh.Set(key, fKey, s)
			if err != nil {
				t.Error(err)
			}

			o, err := rh.Get(key, fKey)
			if err != nil {
				t.Error(err)
			}

			if !reflect.DeepEqual(s, o) {
				t.Error("not equal")
			}

			m, err := rh.GetAll(key)
			if err != nil {
				t.Error(err)
			}

			if _, ok := m[fKey]; !ok {
				t.Error("GetAll failed")
			} else {
				if !reflect.DeepEqual(s, o) {
					t.Error("not equal")
				}
			}

			err = rh.Del(key, fKey)
			if err != nil {
				t.Error(err)
			}
		}(k)
	}

	group.Wait()
}
