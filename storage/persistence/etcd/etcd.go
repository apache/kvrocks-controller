package etcd

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/KvrocksLabs/kvrocks_controller/metadata"

	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"

	"github.com/KvrocksLabs/kvrocks_controller/storage/persistence"

	"github.com/KvrocksLabs/kvrocks_controller/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	sessionTTL         = 6
	defaultDailTimeout = 5 * time.Second
)

type Etcd struct {
	client *clientv3.Client
	kv     clientv3.KV

	leaderMu  sync.RWMutex
	leaderID  string
	myID      string
	electPath string

	quitCh         chan struct{}
	electionCh     chan *concurrency.Election
	leaderChangeCh chan bool
}

func New(id, electPath string, endpoints []string) (*Etcd, error) {
	if len(id) == 0 {
		return nil, errors.New("id must NOT be a empty string")
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: defaultDailTimeout,
		Logger:      logger.Get(),
	})
	if err != nil {
		return nil, err
	}
	e := &Etcd{
		myID:           id,
		electPath:      electPath,
		client:         client,
		kv:             clientv3.NewKV(client),
		quitCh:         make(chan struct{}),
		electionCh:     make(chan *concurrency.Election),
		leaderChangeCh: make(chan bool),
	}
	go e.electLoop(context.Background())
	go e.observeLeaderEvent(context.Background())
	return e, nil
}

func (e *Etcd) ID() string {
	return e.myID
}

func (e *Etcd) Leader() string {
	e.leaderMu.RLock()
	defer e.leaderMu.RUnlock()
	return e.leaderID
}

func (e *Etcd) LeaderChange() <-chan bool {
	return e.leaderChangeCh
}

func (e *Etcd) Get(ctx context.Context, key string) ([]byte, error) {
	rsp, err := e.kv.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(rsp.Kvs) == 0 {
		return nil, metadata.ErrEntryNoExists
	}
	return rsp.Kvs[0].Value, nil
}

func (e *Etcd) Exists(ctx context.Context, key string) (bool, error) {
	_, err := e.Get(ctx, key)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryNoExists) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (e *Etcd) Set(ctx context.Context, key string, value []byte) error {
	_, err := e.kv.Put(ctx, key, string(value))
	return err
}

func (e *Etcd) Delete(ctx context.Context, key string) error {
	_, err := e.kv.Delete(ctx, key)
	return err
}

func (e *Etcd) List(ctx context.Context, prefix string) ([]persistence.Entry, error) {
	rsp, err := e.kv.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	prefixLen := len(prefix)
	entries := make([]persistence.Entry, 0)
	for _, kv := range rsp.Kvs {
		if string(kv.Key) == prefix {
			continue
		}
		value := string(kv.Key)
		entries = append(entries, persistence.Entry{
			Key:   value[prefixLen+1:],
			Value: kv.Value,
		})
	}
	return entries, nil
}

func (e *Etcd) electLoop(ctx context.Context) {
	for {
		select {
		case <-e.quitCh:
			return
		default:
		}

	reset:
		session, err := concurrency.NewSession(e.client, concurrency.WithTTL(sessionTTL))
		if err != nil {
			logger.Get().With(
				zap.Error(err),
			).Error("Failed to create session")
			time.Sleep(sessionTTL / 3)
			continue
		}
		election := concurrency.NewElection(session, e.electPath)
		e.electionCh <- election
		for {
			if err := election.Campaign(ctx, e.myID); err != nil {
				logger.Get().With(
					zap.Error(err),
				).Error("Failed to acquire the leader campaign")
				continue
			}
			select {
			case <-session.Done():
				logger.Get().Warn("Leader session is done")
				goto reset
			case <-e.quitCh:
				logger.Get().Info("Exit the leader election loop")
				return
			}
		}
	}
}

func (e *Etcd) observeLeaderEvent(ctx context.Context) {
	var election *concurrency.Election
	select {
	case elect := <-e.electionCh:
		election = elect
	case <-e.quitCh:
		return
	}

	ch := election.Observe(ctx)
	for {
		select {
		case resp := <-ch:
			if len(resp.Kvs) > 0 {
				newLeaderID := string(resp.Kvs[0].Value)
				e.leaderMu.Lock()
				e.leaderID = newLeaderID
				e.leaderMu.Unlock()
				e.leaderChangeCh <- true
				if newLeaderID != "" && newLeaderID == e.leaderID {
					continue
				}
			} else {
				ch = election.Observe(ctx)
				e.leaderChangeCh <- false
			}
		case elect := <-e.electionCh:
			election = elect
			ch = election.Observe(ctx)
		case <-e.quitCh:
			logger.Get().Info("Exit the leader change observe loop")
			return
		}
	}
}

func (e *Etcd) Close() error {
	close(e.quitCh)
	return e.client.Close()
}
