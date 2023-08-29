package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/RocksLabs/kvrocks_controller/storage/persistence"

	"github.com/RocksLabs/kvrocks_controller/logger"
	"github.com/RocksLabs/kvrocks_controller/metadata"
	"go.uber.org/zap"
)

var (
	ErrNoLeaderOrNotReady = errors.New("the current node role isn't leader or the state is NOT ready")
)

type Storage struct {
	persist persistence.Persistence

	eventNotifyCh chan Event
	quitCh        chan struct{}
}

func NewStorage(persist persistence.Persistence) (*Storage, error) {
	return &Storage{
		persist:       persist,
		eventNotifyCh: make(chan Event, 100),
		quitCh:        make(chan struct{}),
	}, nil
}

func (s *Storage) IsReady() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return s.persist.IsReady(ctx)
}

// ListNamespace return the list of name of all namespaces
func (s *Storage) ListNamespace(ctx context.Context) ([]string, error) {
	entries, err := s.persist.List(ctx, MetadataPrefix)
	if err != nil {
		return nil, err
	}
	keys := make([]string, len(entries))
	for i, entry := range entries {
		keys[i] = entry.Key
	}
	return keys, nil
}

// IsNamespaceExists return an indicator whether the specified namespace exists
func (s *Storage) IsNamespaceExists(ctx context.Context, ns string) (bool, error) {
	return s.persist.Exists(ctx, appendNamespacePrefix(ns))
}

// CreateNamespace will create a namespace for clusters
func (s *Storage) CreateNamespace(ctx context.Context, ns string) error {
	if has, _ := s.IsNamespaceExists(ctx, ns); has {
		return metadata.ErrEntryExisted
	}
	if err := s.persist.Set(ctx, appendNamespacePrefix(ns), []byte(ns)); err != nil {
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
func (s *Storage) RemoveNamespace(ctx context.Context, ns string) error {
	if has, _ := s.IsNamespaceExists(ctx, ns); !has {
		return metadata.ErrEntryNoExists
	}
	clusters, err := s.ListCluster(ctx, ns)
	if err != nil {
		return err
	}
	if len(clusters) != 0 {
		return errors.New("namespace wasn't empty, please remove clusters first")
	}
	if err := s.persist.Delete(ctx, appendNamespacePrefix(ns)); err != nil {
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
func (s *Storage) ListCluster(ctx context.Context, ns string) ([]string, error) {
	entries, err := s.persist.List(ctx, buildClusterPrefix(ns))
	if err != nil {
		return nil, err
	}
	keys := make([]string, len(entries))
	for i, entry := range entries {
		keys[i] = entry.Key
	}
	return keys, nil
}

func (s *Storage) IsClusterExists(ctx context.Context, ns, cluster string) (bool, error) {
	return s.persist.Exists(ctx, buildClusterKey(ns, cluster))
}

func (s *Storage) GetClusterInfo(ctx context.Context, ns, cluster string) (*metadata.Cluster, error) {
	value, err := s.persist.Get(ctx, buildClusterKey(ns, cluster))
	if err != nil {
		return nil, err
	}
	var clusterInfo metadata.Cluster
	if err = json.Unmarshal(value, &clusterInfo); err != nil {
		return nil, err
	}
	return &clusterInfo, nil
}

func (s *Storage) ClusterNodesCounts(ctx context.Context, ns, cluster string) (int, error) {
	clusterInfo, err := s.GetClusterInfo(ctx, ns, cluster)
	if err != nil {
		return -1, err
	}
	count := 0
	for _, shard := range clusterInfo.Shards {
		count += len(shard.Nodes)
	}
	return count, nil
}

// UpdateCluster update the Name to storage under the specified namespace
func (s *Storage) UpdateCluster(ctx context.Context, ns string, clusterInfo *metadata.Cluster) error {
	return s.updateCluster(ctx, ns, clusterInfo)
}

// updateCluster is goroutine unsafe of UpdateCluster
// assumption caller has hold the lock
func (s *Storage) updateCluster(ctx context.Context, ns string, clusterInfo *metadata.Cluster) error {
	if len(clusterInfo.Shards) == 0 {
		return errors.New("required at least one shard")
	}
	value, err := json.Marshal(clusterInfo)
	if err != nil {
		return err
	}
	return s.persist.Set(ctx, buildClusterKey(ns, clusterInfo.Name), value)
}

func (s *Storage) CreateCluster(ctx context.Context, ns string, clusterInfo *metadata.Cluster) error {
	if exists, _ := s.IsClusterExists(ctx, ns, clusterInfo.Name); exists {
		return metadata.ErrEntryExisted
	}
	if err := s.updateCluster(ctx, ns, clusterInfo); err != nil {
		return err
	}
	s.EmitEvent(Event{
		Namespace: ns,
		Cluster:   clusterInfo.Name,
		Type:      EventCluster,
		Command:   CommandCreate,
	})
	return nil
}

func (s *Storage) RemoveCluster(ctx context.Context, ns, cluster string) error {
	if exists, _ := s.IsClusterExists(ctx, ns, cluster); !exists {
		return metadata.ErrEntryNoExists
	}
	if err := s.persist.Delete(ctx, buildClusterKey(ns, cluster)); err != nil {
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

func (s *Storage) Load(ctx context.Context) error {
	namespaces, err := s.ListNamespace(ctx)
	if err != nil {
		return err
	}
	for _, namespace := range namespaces {
		clusters, err := s.ListCluster(ctx, namespace)
		if err != nil {
			return fmt.Errorf("list cluster in namespace[%s] err: %w", namespace, err)
		}
		for _, cluster := range clusters {
			_, err := s.GetClusterInfo(ctx, namespace, cluster)
			if errors.Is(err, metadata.ErrEntryNoExists) {
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
	return nil
}

func (s *Storage) Notify() <-chan Event {
	return s.eventNotifyCh
}

func (s *Storage) EmitEvent(event Event) {
	s.eventNotifyCh <- event
}

func (s *Storage) LeaderChange() <-chan bool {
	return s.persist.LeaderChange()
}

func (s *Storage) IsLeader() bool {
	return s.persist.Leader() == s.persist.ID()
}

func (s *Storage) Leader() string {
	return s.persist.Leader()
}

func (s *Storage) Close() error {
	return s.persist.Close()
}

func (s *Storage) Stop() error {
	return nil
}
