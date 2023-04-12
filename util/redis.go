package util

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	ErrConnFailed = errors.New("redis: connection error")

	pools      map[string]*redis.Client
	poolsMutex *sync.RWMutex
	closeOnce  sync.Once
)

const (
	dialTimeout  = 5 * time.Second
	readTimeout  = 120 * time.Second
	writeTimeout = 120 * time.Second
	maxRetries   = 3
	minIdleConns = 3
)

func NewRedisClient(ctx context.Context, addr string) (*redis.Client, error) {
	if pools == nil {
		pools = make(map[string]*redis.Client)
	}
	if poolsMutex == nil {
		poolsMutex = &sync.RWMutex{}
	}

	inner := func(addr string) (*redis.Client, error) {
		poolsMutex.RLock()
		_, ok := pools[addr]
		poolsMutex.RUnlock()
		if !ok {
			//not exist in map
			client := redis.NewClient(&redis.Options{
				Addr:         addr,
				DialTimeout:  dialTimeout,
				ReadTimeout:  readTimeout,
				WriteTimeout: writeTimeout,
				MaxRetries:   maxRetries,
				MinIdleConns: minIdleConns,
			})
			if err := client.Do(ctx, "ping").Err(); err != nil {
				return nil, err
			}
			poolsMutex.Lock()
			pools[addr] = client
			poolsMutex.Unlock()
		}
		poolsMutex.RLock()
		cli, ok := pools[addr]
		poolsMutex.RUnlock()
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

func CloseRedisClients() error {
	if poolsMutex == nil {
		return nil
	}
	poolsMutex.Lock()
	defer poolsMutex.Unlock()
	closeOnce.Do(func() {
		for _, cli := range pools {
			if cli == nil {
				continue
			}
			cli.Close()
		}
	})
	return nil
}
