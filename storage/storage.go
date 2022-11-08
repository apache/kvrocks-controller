package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/KvrocksLabs/kvrocks_controller/metadata"

	"github.com/KvrocksLabs/kvrocks_controller/logger"
	"github.com/KvrocksLabs/kvrocks_controller/storage/persistence/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

var (
	ErrNoLeaderOrNotReady = errors.New("the current node role isn't leader or the state is NOT ready")
)

type Storage struct {
	instance  *etcd.Etcd
	ready     bool
	myselfID  string
	leaderID  string
	etcdAddrs []string

	electionCh     chan *concurrency.Election
	releaseCh      chan struct{}
	quitCh         chan struct{}
	eventNotifyCh  chan Event
	leaderChangeCh chan bool

	closeOnce sync.Once
	rw        sync.RWMutex
}

// NewStorage create a high level metadata storage
func NewStorage(id string, etcdAddrs []string) (*Storage, error) {
	remote, err := etcd.New(etcdAddrs)
	if err != nil {
		return nil, err
	}
	stor := &Storage{
		instance:       remote,
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

// ListNamespace return the list of name of all namespaces
func (s *Storage) ListNamespace() ([]string, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()

	return s.instance.ListNamespace(context.Background())
}

// HasNamespace return an indicator whether the specified namespace exists
func (s *Storage) HasNamespace(ns string) (bool, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()

	return s.instance.IsNamespaceExists(context.Background(), ns)
}

// CreateNamespace add the specified namespace to storage
func (s *Storage) CreateNamespace(ns string) error {
	s.rw.Lock()
	defer s.rw.Unlock()

	if has, _ := s.instance.IsNamespaceExists(context.Background(), ns); has {
		return metadata.ErrNamespaceHasExisted
	}
	if err := s.instance.CreateNamespace(context.Background(), ns); err != nil {
		return err
	}
	s.EmitEvent(Event{
		Namespace: ns,
		Type:      EventNamespace,
		Command:   CommandCreate,
	})
	return nil
}

// RemoveNamespace delete the specified namespace from storage
func (s *Storage) RemoveNamespace(ns string) error {
	s.rw.Lock()
	defer s.rw.Unlock()

	if has, _ := s.instance.IsNamespaceExists(context.Background(), ns); !has {
		return metadata.ErrNamespaceNoExists
	}
	clusters, err := s.instance.ListCluster(context.Background(), ns)
	if err != nil {
		return err
	}
	if len(clusters) != 0 {
		return errors.New("namespace wasn't empty, please remove clusters first")
	}
	if err := s.instance.RemoveNamespace(ns, context.Background()); err != nil {
		return err
	}
	s.EmitEvent(Event{
		Namespace: ns,
		Type:      EventNamespace,
		Command:   CommandRemove,
	})
	return nil
}

// ListCluster return the list of name of cluster under the specified namespace
func (s *Storage) ListCluster(ns string) ([]string, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()

	return s.instance.ListCluster(context.Background(), ns)
}

func (s *Storage) IsClusterExists(ns, cluster string) (bool, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()

	return s.instance.IsClusterExists(context.Background(), ns, cluster)
}

// GetClusterInfo return a copy of specified 'metadata.Cluster' under the specified namespace
func (s *Storage) GetClusterInfo(ns, cluster string) (metadata.Cluster, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()

	return s.instance.GetCluster(context.Background(), ns, cluster)
}

// ClusterNodesCounts return the count of cluster
func (s *Storage) ClusterNodesCounts(ns, cluster string) (int, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()

	clusterInfo, err := s.instance.GetCluster(context.Background(), ns, cluster)
	if err != nil {
		return -1, err
	}
	count := 0
	for _, shard := range clusterInfo.Shards {
		count += len(shard.Nodes)
	}
	return count, nil
}

// UpdateCluster update the Cluster to storage under the specified namespace
func (s *Storage) UpdateCluster(ns, cluster string, clusterInfo *metadata.Cluster) error {
	s.rw.Lock()
	defer s.rw.Unlock()
	return s.updateCluster(ns, cluster, clusterInfo)
}

// updateCluster is goroutine unsafe of UpdateCluster
// assumption caller has hold the lock
func (s *Storage) updateCluster(ns, cluster string, topo *metadata.Cluster) error {
	if has, _ := s.instance.IsNamespaceExists(context.Background(), ns); !has {
		return metadata.ErrNamespaceNoExists
	}
	if len(topo.Shards) == 0 {
		return errors.New("required at least one shard")
	}
	if err := s.instance.UpdateCluster(context.Background(), ns, cluster, topo); err != nil {
		return err
	}
	return nil
}

// CreateCluster add a Cluster to storage under the specified namespace
func (s *Storage) CreateCluster(ns, cluster string, clusterInfo *metadata.Cluster) error {
	s.rw.Lock()
	defer s.rw.Unlock()

	if has, _ := s.instance.IsClusterExists(context.Background(), ns, cluster); has {
		return metadata.ErrClusterHasExisted
	}
	if err := s.updateCluster(ns, cluster, clusterInfo); err != nil {
		return err
	}
	s.EmitEvent(Event{
		Namespace: ns,
		Cluster:   cluster,
		Type:      EventCluster,
		Command:   CommandCreate,
	})
	return nil
}

// RemoveCluster delete the Cluster from storage under the specified namespace
func (s *Storage) RemoveCluster(ns, cluster string) error {
	s.rw.Lock()
	defer s.rw.Unlock()

	if has, _ := s.instance.IsClusterExists(context.Background(), ns, cluster); !has {
		return metadata.ErrClusterNoExists
	}
	if err := s.instance.RemoveCluster(context.Background(), ns, cluster); err != nil {
		return err
	}
	s.EmitEvent(Event{
		Namespace: ns,
		Cluster:   cluster,
		Type:      EventCluster,
		Command:   CommandRemove,
	})
	return nil
}

func (s *Storage) Load() error {
	namespaces, err := s.instance.ListNamespace(context.Background())
	if err != nil {
		return err
	}
	for _, namespace := range namespaces {
		clusters, err := s.instance.ListCluster(context.Background(), namespace)
		if err != nil {
			return fmt.Errorf("list cluster in namespace[%s] err: %w", namespace, err)
		}
		for _, cluster := range clusters {
			_, err := s.instance.GetCluster(context.Background(), namespace, cluster)
			if errors.Is(err, metadata.ErrClusterNoExists) {
				logger.Get().With(
					zap.Error(err),
					zap.String("cluster", cluster),
				).Warn("Can't load the cluster from storage")
				continue
			}
			if err != nil {
				return fmt.Errorf("get cluster[%s] err: %w", cluster, err)
			}
		}
	}
	s.rw.Lock()
	s.ready = true
	s.rw.Unlock()
	return nil
}

func (s *Storage) Close() error {
	s.rw.Lock()
	defer s.rw.Unlock()
	var err error
	s.closeOnce.Do(func() {
		close(s.quitCh)
		close(s.eventNotifyCh)
		close(s.leaderChangeCh)
		err = s.instance.Close()
	})
	return err
}

func (s *Storage) Stop() error {
	s.rw.Lock()
	defer s.rw.Unlock()
	if s.leaderID != s.myselfID {
		return nil
	}
	s.ready = false
	s.releaseCh <- struct{}{}
	return nil
}

func (s *Storage) LeaderCampaign() {
	for {
		select {
		case <-s.quitCh:
			return
		default:
		}

	reset:
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   s.etcdAddrs,
			DialTimeout: 5 * time.Second,
			Logger:      logger.Get(),
		})
		if err != nil {
			logger.Get().With(
				zap.Error(err),
			).Error("Failed to create the election client")
			continue
		}
		session, err := concurrency.NewSession(client, concurrency.WithTTL(etcd.SessionTTL))
		if err != nil {
			logger.Get().With(
				zap.Error(err),
			).Error("Failed to create session")
			time.Sleep(etcd.ElectInterval)
			continue
		}
		election := concurrency.NewElection(session, etcd.LeaderKey)
		s.electionCh <- election
		for {
			if err := election.Campaign(context.TODO(), s.myselfID); err != nil {
				logger.Get().With(
					zap.Error(err),
				).Error("Failed to acquire the leader campaign")
				continue
			}
			select {
			case <-session.Done():
				logger.Get().Warn("Leader session is done")
				goto reset
			case <-s.releaseCh:
				_ = election.Resign(context.TODO())
				logger.Get().Warn("Leader resign: " + s.myselfID)
				goto reset
			case <-s.quitCh:
				return
			}
		}
	}
}

func (s *Storage) LeaderObserve() {
	var election *concurrency.Election
	select {
	case e := <-s.electionCh:
		election = e
	case <-s.quitCh:
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
				if newLeaderID == s.leaderID {
					logger.Get().Info("I'm the leader now, do nothing")
					continue
				}
				s.setNewLeaderID(newLeaderID)
				if s.leaderChangeCh != nil {
					s.leaderChangeCh <- s.IsLeader()
				}
				logger.Get().Info("Got the new leader: " + s.leaderID)
			} else {
				ch = election.Observe(ctx)
			}
		case e := <-s.electionCh:
			election = e
			ch = election.Observe(ctx)
		case <-s.quitCh:
			return
		}
	}
}

func (s *Storage) setNewLeaderID(id string) {
	s.rw.Lock()
	defer s.rw.Unlock()
	s.leaderID = id
	if s.leaderID != s.myselfID {
		s.ready = false
	}
}

func (s *Storage) Self() string {
	return s.myselfID
}

func (s *Storage) Leader() string {
	s.rw.RLock()
	defer s.rw.RUnlock()
	return s.leaderID
}

func (s *Storage) IsLeader() bool {
	s.rw.RLock()
	defer s.rw.RUnlock()
	return s.myselfID == s.leaderID
}

func (s *Storage) isLeaderAndReady() bool {
	return s.myselfID == s.leaderID && s.ready
}

func (s *Storage) BecomeLeader() <-chan bool {
	return s.leaderChangeCh
}

func (s *Storage) Notify() <-chan Event {
	return s.eventNotifyCh
}

func (s *Storage) EmitEvent(event Event) {
	s.eventNotifyCh <- event
}
