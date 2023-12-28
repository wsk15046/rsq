package redisop

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis/v8"
	"github.com/wsk15046/rsq"
	"github.com/wsk15046/rsq/kerror"
)

type RedisZSet[T any] struct {
	cl redis.UniversalClient
	rsq.ILogger
}

func NewRedisZSet[T any](c redis.UniversalClient, l rsq.ILogger) *RedisZSet[T] {
	return &RedisZSet[T]{cl: c, ILogger: l}
}

func (c *RedisZSet[T]) ZAdd(key string, t *T, score float64) (err *kerror.KError) {

	ctx := context.Background()
	val, e := json.Marshal(t)
	if e != nil {
		return kerror.JsonError.Msg(e.Error())
	}

	e = c.cl.ZAdd(ctx, key, &redis.Z{
		Score:  score,
		Member: val,
	}).Err()

	if e != nil {
		c.Errorf("zadd failed, key[ %s ] err[ %s ]", key, e.Error())
		return kerror.RedisError.Msg(e.Error())
	}

	return nil
}

func (c *RedisZSet[T]) ZCard(key string) (count int64, err *kerror.KError) {
	ctx := context.Background()

	count, e := c.cl.ZCard(ctx, key).Result()
	if e != nil {
		return 0, kerror.RedisError.Msg(e.Error())
	}

	return count, nil
}

func (c *RedisZSet[T]) ZRem(key string, t *T) (count int64, err *kerror.KError) {
	ctx := context.Background()

	val, e := json.Marshal(t)
	if e != nil {
		return 0, kerror.JsonError.Msg(e.Error())
	}

	count, e = c.cl.ZRem(ctx, key, val).Result()
	if e != nil {
		return 0, kerror.RedisError.Msg(e.Error())
	}

	return count, nil
}

func (c *RedisZSet[T]) ZRange(key string, start, end int64, fromSmall bool) (ts []*T, err *kerror.KError) {
	ts = make([]*T, 0)

	ctx := context.Background()

	var ret []string
	var e error

	if fromSmall {
		// score from small to big
		ret, e = c.cl.ZRange(ctx, key, start, end).Result()
		if e != nil {
			c.Errorf("zrange failed, key[%s],err[%s]", key, e.Error())
			return nil, kerror.RedisError.Msg(e.Error())
		}
	} else {
		ret, e = c.cl.ZRevRange(ctx, key, start, end).Result()
		if e != nil {
			c.Errorf("zrevrange failed, key[%s],err[%s]", key, e.Error())
			return nil, kerror.RedisError.Msg(e.Error())
		}
	}

	for _, d := range ret {
		var t T
		e = json.Unmarshal([]byte(d), &t)
		if e != nil {
			c.Warnf("json.Unmarshal failed")
			continue
		}

		ts = append(ts, &t)
	}

	return ts, nil
}

const ZTrimScript = `
	local trimLen = tonumber(ARGV[1])
	local trimSmall = ARGV[2]

	local curLen = redis.call("ZCARD",KEYS[1])

	local r = 0

	if curLen > trimLen then
		if trimSmall == "true" then
			r = redis.call("ZREMRANGEBYRANK",KEYS[1],0,curLen-trimLen-1)
		else
			r = redis.call("ZREMRANGEBYRANK",KEYS[1],trimLen,curLen-1)
		end
	end
	
	return r
`

func (c *RedisZSet[T]) ZTrim(key string, length int64, fromSmall bool) (err *kerror.KError) {

	if length <= 0 {
		return kerror.SystemError.Msgf("invalid ztrim length %d", length)
	}

	ctx := context.Background()

	trimScript := redis.NewScript(ZTrimScript)
	trimSmall := "true"

	if !fromSmall {
		trimSmall = "false"
	}

	_, e := trimScript.Run(ctx, c.cl, []string{key}, length, trimSmall).Result()
	if e != nil {
		return kerror.RedisError.Msg(e.Error())
	}

	return nil
}
