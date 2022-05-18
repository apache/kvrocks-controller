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
	local  *memory.MemStorage
	remote *etcd.EtcdStorage
	ecli   *clientv3.Client // etcd client for election

	myselfID string // server address
	leaderID string // leader address
	election *concurrency.Election // etcd election for leader observe

	eventNotifyCh  chan Event // publish topo update event
	leaderChangeCh chan bool  // publish leader change
	quitCh         chan struct{}
	rw             sync.RWMutex
}

// NewStorage create a high level metadata storage
func NewStorage(id string, etcdAddrs []string) (*Storage, error){
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdAddrs,
		DialTimeout: time.Duration(etcd.EtcdDailTimeout) * time.Second,
		Logger:      logger.Get(),
	}) 
	if err != nil {
		return nil, err
	}
	stor := &Storage {
		local:         memory.NewMemStorage(),
		remote:        etcd.NewEtcdStorage(client),
		ecli:          client,
		myselfID:      id,
		eventNotifyCh: make(chan Event, 100),
		leaderChangeCh:make(chan bool, 1),
		quitCh:        make(chan struct{}),
	}
	go stor.leaderCampaign()
	go stor.leaderMonitor()
	for stor.leaderID == "" {
		continue
	}
	return stor, nil
}

// Close implements io.Closer, terminates the memory storage and etcd storage
func (stor *Storage) Close() error {
	stor.rw.Lock()
	defer stor.rw.Unlock()
	close(stor.quitCh)
	close(stor.eventNotifyCh)
	close(stor.leaderChangeCh)
	// ecli call close, remote must not call close
	if err := stor.ecli.Close(); err != nil {
		return err
	}
	return nil
}

// leaderCampaign propose leader election
func (stor *Storage) leaderCampaign() {
	for {
		session, err := concurrency.NewSession(stor.ecli, concurrency.WithTTL(etcd.SessionTTL))
		if err != nil {
	    	logger.Get().With(
	    		zap.Error(err),
	    	).Error("election leader create session error, current " + stor.myselfID)
	        continue
	    }
	    election := concurrency.NewElection(session, etcd.LeaderKey)
	    stor.election = election
	    if err := election.Campaign(context.TODO(), stor.myselfID); err != nil {
            logger.Get().With(
        		zap.Error(err),
        	).Error("election leader campaign error, current " + stor.myselfID)
            continue
        }
        for {
	        select {
	        case <-session.Done():
	            logger.Get().Error("leader session done, current" + stor.myselfID)
	            break
	        case <-stor.quitCh:
	        	stor.election = nil
	        	session.Close()
	            return
	        }
	    }
	}
}

// leaderMonitor observe leader change 
func (stor *Storage) leaderMonitor() {
	for {
		if stor.election == nil {
			time.Sleep(time.Duration(etcd.MonitorSleep) * time.Second)
			continue
		}
        cctx, cancel := context.WithCancel(context.TODO())
        ch := stor.election.Observe(cctx)
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
                    time.Sleep(time.Duration(etcd.MonitorSleep) * time.Second)
                    cctx, cancel = context.WithCancel(context.TODO())
                    ch = stor.election.Observe(cctx)
                }
	        case <-stor.quitCh:
	            return
            }
        }
    }
} 

// setLeader call by leaderMonitor when leader change
func (stor *Storage) setLeader(id string) {
	stor.rw.Lock()
	defer stor.rw.Unlock()
	stor.leaderID = id
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
	return stor.selfLeaderWithUnLock()
}

// selfLeaderWithUnLock is goroutine unsafety of SelfLeader
// assumption caller has hold the lock
func (stor *Storage) selfLeaderWithUnLock() bool {
	return stor.myselfID == stor.leaderID
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
