package storage

import (
	"time"
	"sync"
	"errors"
	"context"

	"go.uber.org/zap"
	"go.etcd.io/etcd/client/v3"
    "go.etcd.io/etcd/client/v3/concurrency"
	"github.com/KvrocksLabs/kvrocks-controller/logger"
	"github.com/KvrocksLabs/kvrocks-controller/storage/base/etcd"
	"github.com/KvrocksLabs/kvrocks-controller/storage/base/memory"
)

var (
	// ErrSlaveNoSupport is returned if the storage is slave
	// All the reades and writes are disallowed.
	ErrSlaveNoSupport = errors.New("slave not access storage")
)

// Storage implement `MetaStorage` interface, contains all method of metadata
// include local(memory) and remote(etcd) storage, ensure the data consistency
// offer leader election , leader-follower switch and publish topo update event
// (TODO: election stripped from storage into separate module).
type Storage struct {
	local     *memory.MemStorage
	remote 	  *etcd.EtcdStorage
	etcdAddrs []string
	ready     bool

	eventNotifyCh  chan Event // publish topo update event
	leaderChangeCh chan bool  // publish leader change

	myselfID    string // server address
	leaderID    string // leader address
	electionCh  chan *concurrency.Election // election monitor chan
	releaseCh   chan struct{} // election resign chan

	closeOnce sync.Once
	quitCh    chan struct{}
	rw        sync.RWMutex
}

// NewStorage create a high level metadata storage
func NewStorage(id string, etcdAddrs []string) (*Storage, error){
	remote, err := etcd.NewEtcdStorage(etcdAddrs)
	if err != nil {
		return nil, err
	}
	stor := &Storage {
		local:         memory.NewMemStorage(),
		remote:        remote,
		etcdAddrs:     etcdAddrs,
		myselfID:      id,
		eventNotifyCh: make(chan Event, 100),
		leaderChangeCh:make(chan bool, 1),
		electionCh:    make(chan *concurrency.Election, 1),
		releaseCh:     make(chan struct{}, 1),
		quitCh:        make(chan struct{}),
	}
	go stor.LeaderCampaign()
	go stor.LeaderObserve()
	return stor, nil
}

// LoadCluster load namespace and cluster from etcd when start or switch leader 
func (stor *Storage) LoadCluster() error {
	namespcaes , err := stor.remote.ListNamespace()
	if err != nil {
		return err
	}
	memStor := memory.NewMemStorage()
	for _, namespace :=range namespcaes {
		clusters, err := stor.remote.ListCluster(namespace)
		if err != nil {
			return err
		}
		memStor.CreateNamespace(namespace)
		for _, cluster :=range clusters {
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

// Close implements io.Closer, terminates the memory storage and etcd storage
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

// LeaderResgin release leadership
func (stor *Storage) LeaderResign() {
	stor.rw.Lock()
	defer stor.rw.Unlock()
	if stor.leaderID != stor.myselfID {
		return
	}
	stor.ready = false
	stor.releaseCh<- struct{}{}
}

// LeaderCampaign propose leader election
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
               	logger.Get().Warn("leader session done, current" + stor.myselfID)
               	goto reset
	        case <-stor.releaseCh:
	        	election.Resign(context.TODO())
	        	logger.Get().Warn("leader resign " + stor.myselfID)
	        	goto reset
	        case <-stor.quitCh:
	           	return
	        }
	    }
	}
}

// LeaderObserve observe leader change 
func (stor *Storage) LeaderObserve() {
	var election *concurrency.Election
	select {
	case e := <- stor.electionCh:
		election = e
	case <-stor.quitCh:
    	return
	}

	cctx, cancel := context.WithCancel(context.TODO())
    ch := election.Observe(cctx)
    for {
        select {
        case resp := <-ch:
            if len(resp.Kvs) > 0 {
                stor.setLeader(string(resp.Kvs[0].Value))
                if stor.leaderChangeCh != nil {
					stor.leaderChangeCh <- stor.SelfLeader()
				}
				logger.Get().Info("current: " + stor.myselfID + ", change leader: " + stor.leaderID)
            } else {
                cancel()
                cctx, cancel = context.WithCancel(context.TODO())
    			ch = election.Observe(cctx)
            }
        case e := <- stor.electionCh:
        	cancel()
        	election = e
            cctx, cancel = context.WithCancel(context.TODO())
			ch = election.Observe(cctx)
        case <-stor.quitCh:
            return
        }
    }
} 

// setLeader call by LeaderObserve when leader change
func (stor *Storage) setLeader(id string) {
	stor.rw.Lock()
	defer stor.rw.Unlock()
	stor.leaderID = id
	if  stor.leaderID != stor.myselfID {
		stor.ready = false 
	}
	return 
}

// Self return storage id
func (stor *Storage) Self() string {
	return stor.myselfID
}

// Leader return leader id
func (stor *Storage) Leader() string {
	stor.rw.RLock()
	defer stor.rw.RUnlock()
	return stor.leaderID
}

// SelfLeader return whether myself is the leader 
func (stor *Storage) SelfLeader() bool {
	stor.rw.RLock()
	defer stor.rw.RUnlock()
	return stor.myselfID == stor.leaderID
}

// selfLeaderReady is goroutine unsafety of check leader 
// whether ready, assumption caller has hold the lock
func (stor *Storage) selfLeaderReady() bool {
	return stor.myselfID == stor.leaderID && stor.ready
}

// BecomeLeader return chan for publish leader change
func (stor *Storage) BecomeLeader() <-chan bool {
	return stor.leaderChangeCh
}

// Notify return chan for publish topo update event
func (stor *Storage) Notify() <-chan Event {
	return stor.eventNotifyCh
}

// EmitEvent send topo update event to notify chan
func (stor *Storage) EmitEvent(event Event) {
	stor.eventNotifyCh <- event
}
