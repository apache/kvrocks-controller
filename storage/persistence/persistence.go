package persistence

import (
	"context"
)

type Entry struct {
	Key   string
	Value []byte
}

type Persistence interface {
	ID() string
	Leader() string
	LeaderChange() <-chan bool
	IsReady(ctx context.Context) bool

	Get(ctx context.Context, key string) ([]byte, error)
	Exists(ctx context.Context, key string) (bool, error)
	Set(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]Entry, error)

	Close() error
}
