package memory

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/KvrocksLabs/kvrocks-controller/metadata"
	"github.com/KvrocksLabs/kvrocks-controller/storage"
)

type Namespace struct {
	Clusters map[string]*metadata.Cluster
}

type MemStorage struct {
	mu         sync.RWMutex
	namespaces map[string]*Namespace

	eventNotifyCh chan storage.Event
	leaderElectCh chan struct{}
}

func NewMemStorage() *MemStorage {
	return &MemStorage{
		namespaces:    make(map[string]*Namespace),
		eventNotifyCh: make(chan storage.Event),
		leaderElectCh: make(chan struct{}),
	}
}

func (stor *MemStorage) Open() error {
	return nil
}

func (stor *MemStorage) BecomeLeader(id string) (bool, <-chan struct{}) {
	return true, stor.leaderElectCh
}

func (stor *MemStorage) Notify() <-chan storage.Event {
	return stor.eventNotifyCh
}

func (stor *MemStorage) emitEvent(event storage.Event) {
	stor.eventNotifyCh <- event
}

func (stor *MemStorage) Close() error {
	close(stor.leaderElectCh)
	return nil
}

func (stor *MemStorage) ListNamespace() ([]string, error) {
	stor.mu.RLock()
	defer stor.mu.RUnlock()
	namespaces := make([]string, 0, len(stor.namespaces))
	for name := range stor.namespaces {
		namespaces = append(namespaces, name)
	}
	return namespaces, nil
}

func (stor *MemStorage) CreateNamespace(name string) error {
	stor.mu.Lock()
	defer stor.mu.Unlock()
	if namespace, ok := stor.namespaces[name]; ok && namespace != nil {
		return metadata.ErrNamespaceHasExisted
	}
	stor.namespaces[name] = &Namespace{
		Clusters: make(map[string]*metadata.Cluster),
	}
	stor.emitEvent(storage.Event{
		Namespace: name,
		Type:      storage.EventNamespace,
		Command:   storage.CommandCreate,
	})
	return nil
}

func (stor *MemStorage) RemoveNamespace(name string) error {
	stor.mu.Lock()
	defer stor.mu.Unlock()
	if namespace, ok := stor.namespaces[name]; ok && namespace != nil {
		if len(namespace.Clusters) != 0 {
			return errors.New("namespace wasn't empty, please remove clusters first")
		}
		stor.emitEvent(storage.Event{
			Namespace: name,
			Type:      storage.EventNamespace,
			Command:   storage.CommandRemove,
		})
		delete(stor.namespaces, name)
		return nil
	}
	return metadata.ErrNamespaceNoExists
}

func (stor *MemStorage) ListCluster(namespace string) ([]string, error) {
	stor.mu.RLock()
	defer stor.mu.RUnlock()
	ns, ok := stor.namespaces[namespace]
	if !ok {
		return nil, metadata.ErrNamespaceNoExists
	}

	clusterNames := make([]string, 0, len(ns.Clusters))
	for name := range ns.Clusters {
		clusterNames = append(clusterNames, name)
	}
	return clusterNames, nil
}

func (stor *MemStorage) CreateCluster(ns, name string, shards []metadata.Shard) error {
	stor.mu.Lock()
	defer stor.mu.Unlock()
	if namespace, ok := stor.namespaces[ns]; ok {
		if namespace.Clusters == nil {
			stor.namespaces[ns].Clusters = make(map[string]*metadata.Cluster)
		}
		if _, ok := namespace.Clusters[name]; ok {
			return metadata.ErrClusterHasExisted
		}
		if len(shards) == 0 {
			return errors.New("required at least one shard")
		}
		newCluster := &metadata.Cluster{
			Shards:  shards,
			Version: 1,
		}
		stor.namespaces[ns].Clusters[name] = newCluster
		stor.emitEvent(storage.Event{
			Namespace: ns,
			Cluster:   name,
			Type:      storage.EventCluster,
			Command:   storage.CommandCreate,
		})
		return nil
	}
	return metadata.ErrNamespaceNoExists
}

func (stor *MemStorage) GetCluster(ns, name string) (*metadata.Cluster, error) {
	stor.mu.RLock()
	defer stor.mu.RUnlock()
	return stor.GetClusterNoLock(ns, name)
}

func (stor *MemStorage) GetClusterNoLock(ns, name string) (*metadata.Cluster, error) {
	if namespace, ok := stor.namespaces[ns]; ok {
		if namespace.Clusters == nil {
			return nil, metadata.ErrClusterNoExists
		}
		if cluster, ok := namespace.Clusters[name]; !ok {
			return nil, metadata.ErrClusterNoExists
		} else {
			return cluster, nil
		}
	}
	return nil, metadata.ErrNamespaceNoExists
}

func (stor *MemStorage) RemoveCluster(ns, name string) error {
	stor.mu.Lock()
	defer stor.mu.Unlock()
	if namespace, ok := stor.namespaces[ns]; ok {
		if namespace.Clusters == nil {
			return metadata.ErrClusterNoExists
		}
		if _, ok := namespace.Clusters[name]; ok {
			delete(stor.namespaces[ns].Clusters, name)
			stor.emitEvent(storage.Event{
				Namespace: ns,
				Cluster:   name,
				Type:      storage.EventCluster,
				Command:   storage.CommandRemove,
			})
			return nil
		}
		return metadata.ErrClusterNoExists
	}
	return metadata.ErrNamespaceNoExists
}

func (stor *MemStorage) IncrClusterVersion(ns, cluster string) error {
	clusterInfo, err := stor.GetClusterNoLock(ns, cluster)
	if err != nil {
		return err
	}
	atomic.AddInt64(&clusterInfo.Version, 1)
	return nil
}

func (stor *MemStorage) AddShardSlots(ns, cluster string, shardIdx int, slotRanges []metadata.SlotRange) error {
	stor.mu.Lock()
	defer stor.mu.Unlock()

	shard, err := stor.GetShardNoLock(ns, cluster, shardIdx)
	if err != nil {
		return fmt.Errorf("get shard: %w", err)
	}
	if len(shard.Nodes) == 0 {
		return errors.New("the shard was empty, please add Shards first")
	}
	// TODO: merge slot ranges
	_ = stor.IncrClusterVersion(ns, cluster)
	shard.SlotRanges = metadata.MergeSlotRanges(shard.SlotRanges, slotRanges)
	stor.emitEvent(storage.Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     shardIdx,
		Type:      storage.EventShard,
		Command:   storage.CommandAddSlots,
	})
	return nil
}

func (stor *MemStorage) RemoveShardSlots(ns, cluster string, shardIdx int, slotRanges []metadata.SlotRange) error {
	stor.mu.Lock()
	defer stor.mu.Unlock()

	shard, err := stor.GetShardNoLock(ns, cluster, shardIdx)
	if err != nil {
		return fmt.Errorf("get shard: %w", err)
	}
	shard.SlotRanges = metadata.RemoveSlotRanges(shard.SlotRanges, slotRanges)
	stor.emitEvent(storage.Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     shardIdx,
		Type:      storage.EventShard,
		Command:   storage.CommandRemoveSlots,
	})
	return nil
}

func (stor *MemStorage) ListShard(ns, cluster string) ([]metadata.Shard, error) {
	stor.mu.RLock()
	defer stor.mu.RUnlock()

	clusterInfo, err := stor.GetClusterNoLock(ns, cluster)
	if err != nil {
		return nil, fmt.Errorf("get cluster: %w", err)
	}
	shards := make([]metadata.Shard, 0, len(clusterInfo.Shards))
	for i, shard := range clusterInfo.Shards {
		shards[i] = shard
	}
	return shards, nil
}

func (stor *MemStorage) CreateShard(ns, cluster string, shard *metadata.Shard) error {
	stor.mu.Lock()
	defer stor.mu.Unlock()
	c, err := stor.GetClusterNoLock(ns, cluster)
	if err != nil {
		return fmt.Errorf("get cluster: %w", err)
	}
	// It's ok to omit the create event since it didn't modify topology information
	if err := stor.IncrClusterVersion(ns, cluster); err != nil {
		return err
	}
	c.Shards = append(c.Shards, *shard)
	return nil
}

func (stor *MemStorage) GetShard(ns, cluster string, shardIdx int) (*metadata.Shard, error) {
	stor.mu.RLock()
	defer stor.mu.RUnlock()
	return stor.GetShardNoLock(ns, cluster, shardIdx)
}

func (stor *MemStorage) GetShardNoLock(ns, cluster string, shardIdx int) (*metadata.Shard, error) {
	clusterInfo, err := stor.GetClusterNoLock(ns, cluster)
	if err != nil {
		return nil, fmt.Errorf("get cluster: %w", err)
	}
	if clusterInfo.Shards == nil {
		return nil, metadata.NewError("shard", metadata.CodeNoExists, "")
	}
	if shardIdx >= len(clusterInfo.Shards) || shardIdx < 0 {
		return nil, metadata.ErrShardIndexOutOfRange
	}
	return &clusterInfo.Shards[shardIdx], nil
}

func (stor *MemStorage) RemoveShard(ns, cluster string, shardIdx int) error {
	stor.mu.Lock()
	defer stor.mu.Unlock()

	clusterInfo, err := stor.GetClusterNoLock(ns, cluster)
	if err != nil {
		return fmt.Errorf("get cluster: %w", err)
	}
	if shardIdx >= len(clusterInfo.Shards) || shardIdx < 0 {
		return metadata.ErrShardIndexOutOfRange
	}
	shard := clusterInfo.Shards[shardIdx]
	if len(shard.SlotRanges) > 0 {
		return fmt.Errorf("need to delete all slots before removing shard")
	}
	stor.emitEvent(storage.Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     shardIdx,
		Type:      storage.EventShard,
		Command:   storage.CommandRemove,
	})
	_ = stor.IncrClusterVersion(ns, cluster)
	clusterInfo.Shards = append(clusterInfo.Shards[:shardIdx], clusterInfo.Shards[shardIdx+1:]...)
	return nil
}

func (stor *MemStorage) MigrateSlot(ns, cluster, source, target string, slot int) error {
	return nil
}

func (stor *MemStorage) ListNodes(ns, cluster string, shardIdx int) ([]metadata.NodeInfo, error) {
	stor.mu.RLock()
	defer stor.mu.RUnlock()

	shard, err := stor.GetShardNoLock(ns, cluster, shardIdx)
	if err != nil {
		return nil, fmt.Errorf("get shard: %w", err)
	}
	nodes := make([]metadata.NodeInfo, 0, len(shard.Nodes))
	copy(nodes, shard.Nodes)
	return nodes, nil
}

func (stor *MemStorage) CreateNode(ns, cluster string, shardIdx int, node *metadata.NodeInfo) error {
	stor.mu.Lock()
	defer stor.mu.Unlock()

	clusterInfo, err := stor.GetClusterNoLock(ns, cluster)
	if err != nil {
		return fmt.Errorf("get cluster: %w", err)
	}
	if shardIdx >= len(clusterInfo.Shards) || shardIdx < 0 {
		return metadata.ErrShardIndexOutOfRange
	}
	shard := clusterInfo.Shards[shardIdx]
	if shard.Nodes == nil {
		shard.Nodes = make([]metadata.NodeInfo, 0)
	}
	for _, existedNode := range shard.Nodes {
		if existedNode.Address == node.Address {
			return metadata.NewError("existedNode", metadata.CodeExisted, "")
		}
	}
	if len(shard.Nodes) == 0 && !node.IsMaster() {
		return errors.New("you MUST add master node first")
	}
	if len(shard.Nodes) != 0 && node.IsMaster() {
		return errors.New("the master node has already added in this shard")
	}

	shard.Nodes = append(shard.Nodes, *node)
	stor.IncrClusterVersion(ns, cluster)
	clusterInfo.Shards[shardIdx] = shard
	stor.emitEvent(storage.Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     shardIdx,
		NodeID:    node.ID,
		Type:      storage.EventNode,
		Command:   storage.CommandCreate,
	})
	return nil
}

func (stor *MemStorage) RemoveNode(ns, cluster string, shardIdx int, nodeID string) error {
	stor.mu.Lock()
	defer stor.mu.Unlock()

	namespace, ok := stor.namespaces[ns]
	if !ok {
		return metadata.ErrNamespaceNoExists
	}
	clusterInfo, ok := namespace.Clusters[cluster]
	if !ok {
		return metadata.ErrClusterNoExists
	}
	if shardIdx >= len(clusterInfo.Shards) || shardIdx < 0 {
		return metadata.ErrShardIndexOutOfRange
	}
	shard := clusterInfo.Shards[shardIdx]
	if shard.Nodes == nil {
		return metadata.NewError("node", metadata.CodeNoExists, "")
	}
	nodeIdx := -1
	for idx, node := range shard.Nodes {
		if node.ID == nodeID {
			nodeIdx = idx
			break
		}
	}
	if nodeIdx == -1 {
		return metadata.NewError("node", metadata.CodeNoExists, "")
	}
	node := shard.Nodes[nodeIdx]
	if len(shard.SlotRanges) != 0 {
		if len(shard.Nodes) == 1 || node.IsMaster() {
			return errors.New("still some slots in this shard, please migrate them first")
		}
	} else {
		if node.IsMaster() && len(shard.Nodes) > 1 {
			return errors.New("please remove slave Shards first")
		}
	}
	shard.Nodes = append(shard.Nodes[:nodeIdx], shard.Nodes[nodeIdx+1:]...)
	clusterInfo.Shards[shardIdx] = shard
	_ = stor.IncrClusterVersion(ns, cluster)
	stor.emitEvent(storage.Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     shardIdx,
		NodeID:    node.ID,
		Type:      storage.EventNode,
		Command:   storage.CommandRemove,
	})
	return nil
}

func (stor *MemStorage) UpdateNode(ns, cluster string, shardIdx int, node metadata.NodeInfo) error {
	stor.mu.Lock()
	defer stor.mu.Unlock()

	namespace, ok := stor.namespaces[ns]
	if !ok {
		return metadata.ErrNamespaceNoExists
	}
	clusterInfo, ok := namespace.Clusters[cluster]
	if !ok {
		return metadata.NewError("cluster", metadata.CodeNoExists, "")
	}
	if shardIdx >= len(clusterInfo.Shards) || shardIdx < 0 {
		return metadata.ErrShardIndexOutOfRange
	}
	shard := clusterInfo.Shards[shardIdx]
	if shard.Nodes == nil {
		return metadata.ErrNodeNoExists
	}
	// TODO: check the role
	nodeIdx := -1
	for idx, existedNode := range shard.Nodes {
		if existedNode.ID == node.ID {
			stor.emitEvent(storage.Event{
				Namespace: ns,
				Cluster:   cluster,
				Shard:     shardIdx,
				NodeID:    node.ID,
				Type:      storage.EventNode,
				Command:   storage.CommandUpdate,
			})
			shard.Nodes[idx] = node
			clusterInfo.Shards[shardIdx] = shard
			nodeIdx = idx
			break
		}
	}
	if nodeIdx == -1 {
		return metadata.NewError("node", metadata.CodeNoExists, "")
	}
	_ = stor.IncrClusterVersion(ns, cluster)
	return nil
}
