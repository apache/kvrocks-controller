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
	"github.com/KvrocksLabs/kvrocks_controller/storage/persistence/memory"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

var (
	ErrNoLeaderOrNotReady = errors.New("the current node role isn't leader or the state is NOT ready")
)

type Storage struct {
	local  *memory.MemStorage
	remote *etcd.Etcd

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
	remote, err := etcd.New(etcdAddrs)
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

// ListNamespace return the list of name of all namespaces
func (s *Storage) ListNamespace() ([]string, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()
	if !s.isLeaderAndReady() {
		return nil, ErrNoLeaderOrNotReady
	}
	return s.local.ListNamespace()
}

// HasNamespace return an indicator whether the specified namespace exists
func (s *Storage) HasNamespace(ns string) (bool, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()
	if !s.isLeaderAndReady() {
		return false, ErrNoLeaderOrNotReady
	}
	return s.local.HasNamespace(ns)
}

// CreateNamespace add the specified namespace to storage
func (s *Storage) CreateNamespace(ns string) error {
	s.rw.Lock()
	defer s.rw.Unlock()
	if !s.isLeaderAndReady() {
		return ErrNoLeaderOrNotReady
	}
	if has, _ := s.local.HasNamespace(ns); has {
		return metadata.ErrNamespaceHasExisted
	}
	if err := s.remote.CreateNamespace(ns); err != nil {
		return err
	}
	_ = s.local.CreateNamespace(ns)
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
	if !s.isLeaderAndReady() {
		return ErrNoLeaderOrNotReady
	}
	if has, _ := s.local.HasNamespace(ns); !has {
		return metadata.ErrNamespaceNoExists
	}
	clusters, err := s.local.ListCluster(ns)
	if err != nil {
		return err
	}
	if len(clusters) != 0 {
		return errors.New("namespace wasn't empty, please remove clusters first")
	}
	if err := s.remote.RemoveNamespace(ns); err != nil {
		return err
	}
	s.local.RemoveNamespace(ns)
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
	if !s.isLeaderAndReady() {
		return nil, ErrNoLeaderOrNotReady
	}
	return s.local.ListCluster(ns)
}

func (s *Storage) IsClusterExists(ns, cluster string) (bool, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()
	if !s.isLeaderAndReady() {
		return false, ErrNoLeaderOrNotReady
	}
	return s.local.HasCluster(ns, cluster)
}

// GetClusterCopy return a copy of specified 'metadata.Cluster' under the specified namespace
func (s *Storage) GetClusterCopy(ns, cluster string) (metadata.Cluster, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()
	if !s.isLeaderAndReady() {
		return metadata.Cluster{}, ErrNoLeaderOrNotReady
	}
	return s.local.GetClusterCopy(ns, cluster)
}

// ClusterNodesCounts return the count of cluster
func (s *Storage) ClusterNodesCounts(ns, cluster string) (int, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()
	if !s.isLeaderAndReady() {
		return -1, ErrNoLeaderOrNotReady
	}
	clusterInfo, err := s.local.GetClusterCopy(ns, cluster)
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
func (s *Storage) UpdateCluster(ns, cluster string, topo *metadata.Cluster) error {
	s.rw.Lock()
	defer s.rw.Unlock()
	if !s.isLeaderAndReady() {
		return ErrNoLeaderOrNotReady
	}
	return s.updateCluster(ns, cluster, topo)
}

// updateCluster is goroutine unsafety of UpdateCluster
// assumption caller has hold the lock
func (s *Storage) updateCluster(ns, cluster string, topo *metadata.Cluster) error {
	if has, _ := s.local.HasNamespace(ns); !has {
		return metadata.ErrNamespaceNoExists
	}
	if len(topo.Shards) == 0 {
		return errors.New("required at least one shard")
	}
	if err := s.remote.UpdateCluster(ns, cluster, topo); err != nil {
		return err
	}
	s.local.UpdateCluster(ns, cluster, topo)
	return nil
}

// CreateCluster add a Cluster to storage under the specified namespace
func (s *Storage) CreateCluster(ns, cluster string, topo *metadata.Cluster) error {
	s.rw.Lock()
	defer s.rw.Unlock()
	if !s.isLeaderAndReady() {
		return ErrNoLeaderOrNotReady
	}
	if has, _ := s.local.HasCluster(ns, cluster); has {
		return metadata.ErrClusterHasExisted
	}
	if err := s.updateCluster(ns, cluster, topo); err != nil {
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
	if !s.isLeaderAndReady() {
		return ErrNoLeaderOrNotReady
	}
	if has, _ := s.local.HasNamespace(ns); !has {
		return metadata.ErrNamespaceNoExists
	}
	if has, _ := s.local.HasCluster(ns, cluster); !has {
		return metadata.ErrClusterNoExists
	}
	if err := s.remote.RemoveCluster(ns, cluster); err != nil {
		return err
	}
	_ = s.local.RemoveCluster(ns, cluster)
	s.EmitEvent(Event{
		Namespace: ns,
		Cluster:   cluster,
		Type:      EventCluster,
		Command:   CommandRemove,
	})
	return nil
}

func (s *Storage) LoadTasks() error {
	namespaces, err := s.remote.ListNamespace()
	if err != nil {
		return err
	}
	memStor := memory.NewMemStorage()
	for _, namespace := range namespaces {
		clusters, err := s.remote.ListCluster(namespace)
		if err != nil {
			return fmt.Errorf("list cluster in namespace[%s] err: %w", namespace, err)
		}
		memStor.CreateNamespace(namespace)
		for _, cluster := range clusters {
			topo, err := s.remote.GetClusterCopy(namespace, cluster)
			if errors.Is(err, metadata.ErrClusterNoExists) {
				logger.Get().With(zap.String("cluster", cluster)).
					Warn("Can't load the cluster from storage")
				continue
			}
			if err != nil {
				return fmt.Errorf("get cluster[%s] err: %w", cluster, err)
			}
			memStor.CreateCluster(namespace, cluster, &topo)
		}
	}
	s.rw.Lock()
	s.local = memStor
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
		err = s.remote.Close()
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
				s.setNewLeaderID(string(resp.Kvs[0].Value))
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
