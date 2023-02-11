package etcd

import (
	"context"
	"time"

	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"

	"github.com/KvrocksLabs/kvrocks_controller/storage/persistence"

	"github.com/KvrocksLabs/kvrocks_controller/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	SessionTTL         = 15
	ElectInterval      = 3 * time.Second
	defaultDailTimeout = 5 * time.Second

	LeaderKey = "kvrocks-controller-leader"
)

type Etcd struct {
	client *clientv3.Client
	kv     clientv3.KV

	myID           string
	isLeader       bool
	electionCh     chan *concurrency.Election
	releaseCh      chan struct{}
	quitCh         chan struct{}
	leaderChangeCh chan bool
}

func New(endpoints []string) (*Etcd, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: defaultDailTimeout,
		Logger:      logger.Get(),
	})
	if err != nil {
		return nil, err
	}
	return &Etcd{
		client: client,
		kv:     clientv3.NewKV(client),
	}, nil
}

func (e *Etcd) Close() error {
	return e.client.Close()
}

func (e *Etcd) Get(ctx context.Context, key string) ([]byte, error) {
	rsp, err := e.kv.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(rsp.Kvs) == 0 {
		return nil, persistence.ErrKeyNotFound
	}
	return rsp.Kvs[0].Value, nil
}

func (e *Etcd) Exists(ctx context.Context, key string) (bool, error) {
	_, err := e.Get(ctx, key)
	if err != nil {
		if err == persistence.ErrKeyNotFound {
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

func (e *Etcd) LeaderCampaign() {
	for {
		select {
		case <-e.quitCh:
			return
		default:
		}

	reset:
		session, err := concurrency.NewSession(e.client, concurrency.WithTTL(SessionTTL))
		if err != nil {
			logger.Get().With(
				zap.Error(err),
			).Error("Failed to create session")
			time.Sleep(ElectInterval)
			continue
		}
		election := concurrency.NewElection(session, LeaderKey)
		e.electionCh <- election
		for {
			if err := election.Campaign(context.TODO(), e.myID); err != nil {
				logger.Get().With(
					zap.Error(err),
				).Error("Failed to acquire the leader campaign")
				continue
			}
			select {
			case <-session.Done():
				logger.Get().Warn("Leader session is done")
				goto reset
			case <-e.releaseCh:
				_ = election.Resign(context.TODO())
				logger.Get().Warn("Leader resign: " + e.myID)
				goto reset
			case <-e.quitCh:
				return
			}
		}
	}
}

func (e *Etcd) LeaderObserve() {
	var election *concurrency.Election
	select {
	case elect := <-e.electionCh:
		election = elect
	case <-e.quitCh:
		return
	}

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	ch := election.Observe(ctx)
	for {
		select {
		case resp := <-ch:
			if len(resp.Kvs) > 0 {
				newLeaderID := string(resp.Kvs[0].Value)
				if newLeaderID == e.myID {
					logger.Get().Info("I'm the leader now, do nothing")
					continue
				}
				logger.Get().Info("Got the new leader: " + newLeaderID)
			} else {
				ch = election.Observe(ctx)
			}
		case elect := <-e.electionCh:
			election = elect
			ch = election.Observe(ctx)
		case <-e.quitCh:
			return
		}
	}
}
