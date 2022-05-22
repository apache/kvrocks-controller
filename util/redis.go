package util

import (
	"sync"
	"time"
	"errors"
	"context"

	"github.com/go-redis/redis/v8"
)

var (
	ErrConnFailed  = errors.New("redis: connection error")

	poolMap        map[string]*redis.Client //redis connection pool for each server
	poolMutex      *sync.RWMutex
	closeOnce      sync.Once
)

const (
	CONN_TIMEOUT  = 5 *   time.Second
	READ_TIMEOUT  = 120 * time.Second
	WRITE_TIMEOUT = 120 * time.Second
	NUM_RETRY     = 3
	IDLE_CONNS    = 3
)

func RedisPool(addr string)(*redis.Client, error) {
	if poolMap == nil {
		poolMap = make(map[string]*redis.Client)
	}
	if poolMutex == nil {
		poolMutex = &sync.RWMutex{}
	}

	inner := func(addr string) (*redis.Client, error) {
		poolMutex.RLock()
		_, ok := poolMap[addr]
		poolMutex.RUnlock()
		if !ok {
			//not exist in map
			client := redis.NewClient(&redis.Options{
				Addr:         addr,
				DialTimeout:  CONN_TIMEOUT,
				ReadTimeout:  READ_TIMEOUT,
				WriteTimeout: WRITE_TIMEOUT,
				MaxRetries:   NUM_RETRY,
				MinIdleConns: IDLE_CONNS,
			})
			if err := client.Do(context.Background(), "ping").Err(); err != nil {
				return nil, err
			}
			poolMutex.Lock()
			poolMap[addr] = client
			poolMutex.Unlock()
		}
		poolMutex.RLock()
		cli, ok := poolMap[addr]
		poolMutex.RUnlock()
		if ok {
			return cli, nil
		} else {
			return nil, ErrConnFailed
		}
	}
	resp, err := inner(addr)
	if err == nil {
		return resp, nil
	}
	return nil, err
}

func RedisPoolClose() error {
	poolMutex.Lock()
	defer poolMutex.Unlock()
	closeOnce.Do(func(){
		for _, cli :=range poolMap {
			cli.Close()
		}
	})
	return nil
}
