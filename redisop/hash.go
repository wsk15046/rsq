package redisop

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis/v8"
	"github.com/wsk15046/rsq"
	"github.com/wsk15046/rsq/kerror"
)

type RedisHash[T any] struct {
	cl redis.UniversalClient
	rsq.ILogger
}

func NewRedisHash[T any](c redis.UniversalClient, l rsq.ILogger) *RedisHash[T] {
	return &RedisHash[T]{cl: c, ILogger: l}
}

func (c *RedisHash[T]) GetAll(key string) (m map[string]*T, err *kerror.KError) {

	ctx := context.Background()
	m = make(map[string]*T)

	rm, e := c.cl.HGetAll(ctx, key).Result()
	if e != nil {
		return nil, kerror.RedisError.Msg(e.Error())
	}

	for hashKey, hashVal := range rm {
		k := hashKey
		v := new(T)
		if e = json.Unmarshal([]byte(hashVal), v); e != nil {
			c.Errorf("Unmarshal failed, key[ %s ], hashKey[ %s ], hashVal[ %v ], err[ %s ]",
				key, hashKey, hashVal, e.Error())
			continue
		}
		m[k] = v
	}

	return m, nil
}

func (c *RedisHash[T]) Set(key, fkey string, t *T) (err *kerror.KError) {

	ctx := context.Background()

	hashVal, e := json.Marshal(t)
	if e != nil {
		return kerror.JsonError.Msg(e.Error())
	}

	if e = c.cl.HSet(ctx, key, fkey, hashVal).Err(); e != nil {
		return kerror.RedisError.Msg(e.Error())
	}

	return nil
}

func (c *RedisHash[T]) Get(key, fkey string) (t *T, codeError *kerror.KError) {
	ctx := context.Background()
	t = new(T)

	b, e := c.cl.HGet(ctx, key, fkey).Bytes()
	if e != nil {
		return nil, kerror.RedisError.Msg(e.Error())
	}

	e = json.Unmarshal(b, t)
	if e != nil {
		return nil, kerror.JsonError.Msg(e.Error())
	}

	return t, nil
}

func (c *RedisHash[T]) Del(key, fkey string) (err *kerror.KError) {
	ctx := context.Background()

	if e := c.cl.HDel(ctx, key, fkey).Err(); e != nil {
		c.Errorf("redis HDel failed, key[ %s ], hashKey[ %s ] err[ %s ]", key, fkey, e.Error())
		return kerror.RedisError.Msg(e.Error())
	}

	return nil
}

func (c *RedisHash[T]) DelAll(key string) (err *kerror.KError) {
	ctx := context.Background()

	if e := c.cl.Del(ctx, key).Err(); e != nil {
		c.Errorf("redis del failed, key[ %s ]err[ %s ]", key, e.Error())
		return kerror.RedisError.Msg(e.Error())
	}

	return nil
}
