package storage

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/KvrocksLabs/kvrocks_controller/logger"
	"github.com/KvrocksLabs/kvrocks_controller/storage/base/etcd"
	"github.com/KvrocksLabs/kvrocks_controller/storage/base/memory"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

var (
	ErrSlaveNoSupport = errors.New("slave not access storage")
)

type Storage struct {
	local  *memory.MemStorage
	remote *etcd.EtcdStorage

	etcdAddrs []string
	ready     bool

	eventNotifyCh  chan Event
	leaderChangeCh chan bool

	myselfID   string
	leaderID   string
	electionCh chan *concurrency.Election
	releaseCh  chan struct{}

	closeOnce sync.Once
	quitCh    chan struct{}
	rw        sync.RWMutex
}

// NewStorage create a high level metadata storage
func NewStorage(id string, etcdAddrs []string) (*Storage, error) {
	remote, err := etcd.NewEtcdStorage(etcdAddrs)
	if err != nil {
		return nil, err
	}
	stor := &Storage{
		local:          memory.NewMemStorage(),
		remote:         remote,
		etcdAddrs:      etcdAddrs,
		myselfID:       id,
		eventNotifyCh:  make(chan Event, 100),
		leaderChangeCh: make(chan bool, 1),
		electionCh:     make(chan *concurrency.Election, 1),
		releaseCh:      make(chan struct{}, 1),
		quitCh:         make(chan struct{}),
	}
	go stor.LeaderCampaign()
	go stor.LeaderObserve()
	return stor, nil
}

func (stor *Storage) LoadTasks() error {
	namespaces, err := stor.remote.ListNamespace()
	if err != nil {
		return err
	}
	memStor := memory.NewMemStorage()
	for _, namespace := range namespaces {
		clusters, err := stor.remote.ListCluster(namespace)
		if err != nil {
			return err
		}
		memStor.CreateNamespace(namespace)
		for _, cluster := range clusters {
			topo, err := stor.remote.GetClusterCopy(namespace, cluster)
			if err != nil {
				return nil
			}
			memStor.CreateCluster(namespace, cluster, &topo)
		}
	}
	stor.rw.Lock()
	defer stor.rw.Unlock()
	stor.local = memStor
	stor.ready = true
	return nil
}

func (stor *Storage) Close() error {
	stor.rw.Lock()
	defer stor.rw.Unlock()
	var err error
	stor.closeOnce.Do(func() {
		close(stor.quitCh)
		close(stor.eventNotifyCh)
		close(stor.leaderChangeCh)
		err = stor.remote.Close()
	})
	return err
}

func (stor *Storage) Stop() error {
	stor.rw.Lock()
	defer stor.rw.Unlock()
	if stor.leaderID != stor.myselfID {
		return nil
	}
	stor.ready = false
	stor.releaseCh <- struct{}{}
	return nil
}

func (stor *Storage) LeaderCampaign() {
	for {
		select {
		case <-stor.quitCh:
			return
		default:
		}
	reset:
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   stor.etcdAddrs,
			DialTimeout: time.Duration(etcd.EtcdDailTimeout) * time.Second,
			Logger:      logger.Get(),
		})
		if err != nil {
			logger.Get().With(
				zap.Error(err),
			).Error("create election client error")
			continue
		}
		session, err := concurrency.NewSession(client, concurrency.WithTTL(etcd.SessionTTL))
		if err != nil {
			logger.Get().With(
				zap.Error(err),
			).Error("election leader create session error, current " + stor.myselfID)
			time.Sleep(time.Duration(etcd.MonitorSleep) * time.Second)
			continue
		}
		election := concurrency.NewElection(session, etcd.LeaderKey)
		stor.electionCh <- election
		for {
			if err := election.Campaign(context.TODO(), stor.myselfID); err != nil {
				logger.Get().With(
					zap.Error(err),
				).Error("election leader campaign error, current " + stor.myselfID)
				continue
			}
			select {
			case <-session.Done():
				logger.Get().Warn("leader session done, current " + stor.myselfID)
				goto reset
			case <-stor.releaseCh:
				_ = election.Resign(context.TODO())
				logger.Get().Warn("leader resign " + stor.myselfID)
				goto reset
			case <-stor.quitCh:
				return
			}
		}
	}
}

func (stor *Storage) LeaderObserve() {
	var election *concurrency.Election
	select {
	case e := <-stor.electionCh:
		election = e
	case <-stor.quitCh:
		return
	}

	cctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	ch := election.Observe(cctx)
	for {
		select {
		case resp := <-ch:
			if len(resp.Kvs) > 0 {
				stor.setLeader(string(resp.Kvs[0].Value))
				if stor.leaderChangeCh != nil {
					stor.leaderChangeCh <- stor.IsLeader()
				}
				logger.Get().Info("current: " + stor.myselfID + ", change leader: " + stor.leaderID)
			} else {
				ch = election.Observe(cctx)
			}
		case e := <-stor.electionCh:
			election = e
			ch = election.Observe(cctx)
		case <-stor.quitCh:
			return
		}
	}
}

func (stor *Storage) setLeader(id string) {
	stor.rw.Lock()
	defer stor.rw.Unlock()
	stor.leaderID = id
	if stor.leaderID != stor.myselfID {
		stor.ready = false
	}
}

func (stor *Storage) Self() string {
	return stor.myselfID
}

func (stor *Storage) Leader() string {
	stor.rw.RLock()
	defer stor.rw.RUnlock()
	return stor.leaderID
}

func (stor *Storage) IsLeader() bool {
	stor.rw.RLock()
	defer stor.rw.RUnlock()
	return stor.myselfID == stor.leaderID
}

func (stor *Storage) selfLeaderReady() bool {
	return stor.myselfID == stor.leaderID && stor.ready
}

func (stor *Storage) BecomeLeader() <-chan bool {
	return stor.leaderChangeCh
}

func (stor *Storage) Notify() <-chan Event {
	return stor.eventNotifyCh
}

func (stor *Storage) EmitEvent(event Event) {
	stor.eventNotifyCh <- event
}
