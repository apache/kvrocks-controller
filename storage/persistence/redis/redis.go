package redis

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/kvrocks-controller/storage/persistence"
	"github.com/go-redis/redis/v8"
	"go.etcd.io/etcd/clientv3/concurrency"
)

const (
	sessionTTL         = 6
	defaultDailTimeout = 5 * time.Second
)

const defaultElectPath = "/kvrocks/controller/leader"

type Config struct {
	Addrs     string `yaml:"addrs"`
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
	DB        int    `yaml:"db"`
	ElectPath string `yaml:"elect_path"`
}

type Redis struct {
	client *redis.Client

	leaderMu  sync.RWMutex
	leaderID  string
	myID      string
	electPath string
	isReady   atomic.Bool

	quitCh         chan struct{}
	electionCh     chan *concurrency.Election
	leaderChangeCh chan bool
}

func New(id string, cfg *Config) (*Redis, error) {
	if len(id) == 0 {
		return nil, errors.New("id must NOT be a empty string")
	}

	clientConfig := &redis.Options{
		Addr:     cfg.Addrs,
		Username: cfg.Username,
		Password: cfg.Password,
		DB:       cfg.DB,
	}

	client := redis.NewClient(clientConfig)

	electPath := defaultElectPath
	if cfg.ElectPath != "" {
		electPath = cfg.ElectPath
	}
	e := &Redis{
		myID:           id,
		electPath:      electPath,
		client:         client,
		quitCh:         make(chan struct{}),
		electionCh:     make(chan *concurrency.Election),
		leaderChangeCh: make(chan bool),
	}
	e.isReady.Store(false)
	return e, nil
}

func (e *Redis) Get(ctx context.Context, key string) ([]byte, error) {
	resp := e.client.Get(ctx, key)
	if resp.Err() != nil {
		return nil, resp.Err()
	}

	return []byte(resp.Val()), nil
}

func (e *Redis) Exists(ctx context.Context, key string) (bool, error) {
	resp := e.client.Exists(ctx, key)
	if resp.Err() != nil {
		return false, resp.Err()
	}
	return resp.Val() > 0, nil
}

func (e *Redis) Set(ctx context.Context, key string, value []byte) error {
	resp := e.client.Set(ctx, key, string(value), 0)
	if resp.Err() != nil {
		return resp.Err()
	}
	return nil
}

func (e *Redis) Delete(ctx context.Context, key string) error {
	resp := e.client.Del(ctx, key)
	if resp.Err() != nil {
		return resp.Err()
	}
	return nil
}

func (e *Redis) List(ctx context.Context, prefix string) ([]persistence.Entry, error) {
	entries := make([]persistence.Entry, 0)
	next, err := e.Scan(ctx, 0, prefix, entries)
	if err != nil {
		return entries, err
	}
	for next != 0 {
		next, err = e.Scan(ctx, 0, prefix, entries)
		if err != nil {
			return entries, err
		}
	}
	return entries, nil
}

func (e *Redis) Scan(ctx context.Context, beginCursor uint64, prefix string, entries []persistence.Entry) (uint64, error) {
	resp := e.client.Scan(ctx, beginCursor, prefix, 20)
	if resp.Err() != nil {
		return 0, resp.Err()
	}
	keys, cursor := resp.Val()
	prefixLen := len(prefix)
	for _, kv := range keys {
		if string(kv) == prefix {
			continue
		}
		key := strings.TrimLeft(string(kv[prefixLen+1:]), "/")
		if strings.ContainsRune(key, '/') {
			continue
		}
		value, err := e.Get(ctx, key)
		if err == nil {
			entries = append(entries, persistence.Entry{
				Key:   key,
				Value: value,
			})
		}

	}
	return cursor, nil

}
