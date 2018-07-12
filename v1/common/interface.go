package common

import (
	"time"
	"gopkg.in/redis.v5"
)

type RedisClientInterface interface {
	Ping() *redis.StatusCmd
	Get(key string) *redis.StringCmd
	MGet(keys ...string) *redis.SliceCmd
	Set(key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Append(key, value string) *redis.IntCmd
	Del(keys ...string) *redis.IntCmd
	ZAdd(key string, members ...redis.Z) *redis.IntCmd
	LPop(key string) *redis.StringCmd
	RPush(key string, values ...interface{}) *redis.IntCmd
	LRange(key string, start, stop int64) *redis.StringSliceCmd
	BLPop(timeout time.Duration, keys ...string) *redis.StringSliceCmd
	BRPop(timeout time.Duration, keys ...string) *redis.StringSliceCmd
}