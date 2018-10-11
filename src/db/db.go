package db

import (
	"github.com/garyburd/redigo/redis"
	"time"
)

// Создание db пула
func CreateRedisPool(addr string, maxIdle int) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     maxIdle,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", addr)
		},
		Wait: true,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}
