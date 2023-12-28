package redisop

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis/v8"
	"github.com/wsk15046/rsq"
	"github.com/wsk15046/rsq/kerror"
	"time"
)

type RedisString[T any] struct {
	cl redis.UniversalClient
	rsq.ILogger
}

func NewRedisString[T any](c redis.UniversalClient, l rsq.ILogger) *RedisString[T] {
	return &RedisString[T]{cl: c, ILogger: l}
}

func (c *RedisString[T]) Set(key string, t *T, expiration time.Duration) (err *kerror.KError) {

	ctx := context.Background()

	val, e := json.Marshal(t)
	if e != nil {
		return kerror.JsonError.Msg(e.Error())
	}

	if e = c.cl.Set(ctx, key, val, expiration).Err(); e != nil {
		c.Errorf("set failed, key[ %s ] err[ %s ]", key, e.Error())
		return kerror.RedisError.Msg(e.Error())
	}

	return nil
}

func (c *RedisString[T]) Get(key string) (t *T, codeError *kerror.KError) {
	ctx := context.Background()
	t = new(T)

	b, e := c.cl.Get(ctx, key).Bytes()
	if e != nil {
		c.Errorf("get failed, key[ %s ] err[ %s ]", key, e.Error())
		return nil, kerror.RedisError.Msg(e.Error())
	}

	e = json.Unmarshal(b, t)
	if e != nil {
		return nil, kerror.JsonError.Msg(e.Error())
	}

	return t, nil
}

func (c *RedisString[T]) Del(key string) (err *kerror.KError) {
	ctx := context.Background()

	if e := c.cl.Del(ctx, key).Err(); e != nil {
		c.Errorf("del failed, key[ %s ] err[ %s ]", key, e.Error())
		return kerror.RedisError.Msg(e.Error())
	}

	return nil
}
