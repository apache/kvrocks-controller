package memory

import (
	"errors"
	"sync"

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

func (memStorage *MemStorage) Open() error {
	return nil
}

func (memStorage *MemStorage) BecomeLeader(id string) (bool, <-chan struct{}) {
	return true, memStorage.leaderElectCh
}

func (memStorage *MemStorage) Notify() <-chan storage.Event {
	return memStorage.eventNotifyCh
}

func (memStorage *MemStorage) emitEvent(event storage.Event) {
	memStorage.eventNotifyCh <- event
}

func (memStorage *MemStorage) Close() error {
	close(memStorage.leaderElectCh)
	return nil
}

func (memStorage *MemStorage) ListNamespace() ([]string, error) {
	memStorage.mu.RLock()
	defer memStorage.mu.RUnlock()
	namespaces := make([]string, 0, len(memStorage.namespaces))
	for name := range memStorage.namespaces {
		namespaces = append(namespaces, name)
	}
	return namespaces, nil
}

func (memStorage *MemStorage) CreateNamespace(name string) error {
	memStorage.mu.Lock()
	defer memStorage.mu.Unlock()
	if namespace, ok := memStorage.namespaces[name]; ok && namespace != nil {
		return metadata.ErrNamespaceHasExisted
	}
	memStorage.namespaces[name] = &Namespace{
		Clusters: make(map[string]*metadata.Cluster),
	}
	memStorage.emitEvent(storage.Event{
		Namespace: name,
		Type:      storage.EventNamespace,
		Command:   storage.CommandCreate,
	})
	return nil
}

func (memStorage *MemStorage) RemoveNamespace(name string) error {
	memStorage.mu.Lock()
	defer memStorage.mu.Unlock()
	if namespace, ok := memStorage.namespaces[name]; ok && namespace != nil {
		if len(namespace.Clusters) != 0 {
			return errors.New("namespace wasn't empty, please remove clusters first")
		}
		memStorage.emitEvent(storage.Event{
			Namespace: name,
			Type:      storage.EventNamespace,
			Command:   storage.CommandRemove,
		})
		delete(memStorage.namespaces, name)
		return nil
	}
	return metadata.ErrNamespaceNoExists
}

func (memStorage *MemStorage) ListCluster(namespace string) ([]string, error) {
	memStorage.mu.RLock()
	defer memStorage.mu.RUnlock()
	ns, ok := memStorage.namespaces[namespace]
	if !ok {
		return nil, metadata.ErrNamespaceNoExists
	}

	clusterNames := make([]string, 0, len(ns.Clusters))
	for name := range ns.Clusters {
		clusterNames = append(clusterNames, name)
	}
	return clusterNames, nil
}

func (memStorage *MemStorage) CreateCluster(ns, name string, shards []metadata.Shard) error {
	memStorage.mu.Lock()
	defer memStorage.mu.Unlock()
	if namespace, ok := memStorage.namespaces[ns]; ok {
		if namespace.Clusters == nil {
			memStorage.namespaces[ns].Clusters = make(map[string]*metadata.Cluster)
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
		memStorage.namespaces[ns].Clusters[name] = newCluster
		memStorage.emitEvent(storage.Event{
			Namespace: ns,
			Cluster:   name,
			Type:      storage.EventCluster,
			Command:   storage.CommandCreate,
		})
		return nil
	}
	return metadata.ErrNamespaceNoExists
}

func (memStorage *MemStorage) GetCluster(ns, name string) (*metadata.Cluster, error) {
	memStorage.mu.RLock()
	defer memStorage.mu.RUnlock()
	if namespace, ok := memStorage.namespaces[ns]; ok {
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

func (memStorage *MemStorage) RemoveCluster(ns, name string) error {
	memStorage.mu.Lock()
	defer memStorage.mu.Unlock()
	if namespace, ok := memStorage.namespaces[ns]; ok {
		if namespace.Clusters == nil {
			return metadata.ErrClusterNoExists
		}
		if _, ok := namespace.Clusters[name]; ok {
			delete(memStorage.namespaces[ns].Clusters, name)
			memStorage.emitEvent(storage.Event{
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

func (memStorage *MemStorage) AddShardSlots(ns, cluster string, shardIdx int, slotRanges []metadata.SlotRange) error {
	c, err := memStorage.GetCluster(ns, cluster)
	if err != nil {
		return err
	}
	for _, slotRange := range slotRanges {
		if err := c.CheckOverlap(&slotRange); err != nil {
			return err
		}
	}
	s, err := memStorage.GetShard(ns, cluster, shardIdx)
	if err != nil {
		return err
	}
	if len(s.Nodes) == 0 {
		return errors.New("the shard was empty, please add Shards first")
	}
	// TODO: merge slot ranges
	s.SlotRanges = append(s.SlotRanges, slotRanges...)
	memStorage.emitEvent(storage.Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     shardIdx,
		Type:      storage.EventShard,
		Command:   storage.CommandAddSlots,
	})
	return nil
}

func (memStorage *MemStorage) ListShard(ns, cluster string) ([]metadata.Shard, error) {
	memStorage.mu.RLock()
	defer memStorage.mu.RUnlock()

	namespace, ok := memStorage.namespaces[ns]
	if !ok {
		return nil, metadata.ErrNamespaceNoExists
	}
	c, ok := namespace.Clusters[cluster]
	if !ok {
		return nil, metadata.ErrClusterNoExists
	}
	shards := make([]metadata.Shard, 0, len(c.Shards))
	for i, shard := range c.Shards {
		shards[i] = shard
	}
	return shards, nil
}

func (memStorage *MemStorage) CreateShard(ns, cluster string, shard *metadata.Shard) error {
	memStorage.mu.Lock()
	defer memStorage.mu.Unlock()

	namespace, ok := memStorage.namespaces[ns]
	if !ok {
		return metadata.ErrNamespaceNoExists
	}
	c, ok := namespace.Clusters[cluster]
	if !ok {
		return metadata.ErrClusterNoExists
	}
	if c.Shards == nil {
		c.Shards = make([]metadata.Shard, 0)
	}
	memStorage.emitEvent(storage.Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     len(c.Shards),
		Type:      storage.EventShard,
		Command:   storage.CommandCreate,
	})
	c.Shards = append(c.Shards, *shard)
	return nil
}

func (memStorage *MemStorage) GetShard(ns, cluster string, shardIdx int) (*metadata.Shard, error) {
	memStorage.mu.RLock()
	defer memStorage.mu.RUnlock()

	namespace, ok := memStorage.namespaces[ns]
	if !ok {
		return nil, metadata.ErrNamespaceNoExists
	}
	c, ok := namespace.Clusters[cluster]
	if !ok {
		return nil, metadata.ErrClusterNoExists
	}
	if c.Shards == nil {
		return nil, metadata.NewError("shard", metadata.CodeNoExists, "")
	}
	if shardIdx >= len(c.Shards) || shardIdx < 0 {
		return nil, metadata.ErrShardIndexOutOfRange
	}
	return &c.Shards[shardIdx], nil
}

func (memStorage *MemStorage) RemoveShard(ns, cluster string, shardIdx int) error {
	memStorage.mu.Lock()
	defer memStorage.mu.Unlock()

	namespace, ok := memStorage.namespaces[ns]
	if !ok {
		return metadata.ErrNamespaceNoExists
	}
	c, ok := namespace.Clusters[cluster]
	if !ok {
		return metadata.ErrClusterNoExists
	}
	if shardIdx >= len(c.Shards) || shardIdx < 0 {
		return metadata.ErrShardIndexOutOfRange
	}
	memStorage.emitEvent(storage.Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     shardIdx,
		Type:      storage.EventShard,
		Command:   storage.CommandRemove,
	})
	c.Shards = append(c.Shards[:shardIdx], c.Shards[shardIdx+1:]...)
	return nil
}

func (memStorage *MemStorage) MigrateSlot(ns, cluster, source, target string, slot int) error {
	return nil
}

func (memStorage *MemStorage) ListNodes(ns, cluster string, shardIdx int) ([]metadata.NodeInfo, error) {
	memStorage.mu.RLock()
	defer memStorage.mu.RUnlock()

	namespace, ok := memStorage.namespaces[ns]
	if !ok {
		return nil, metadata.ErrNamespaceNoExists
	}
	c, ok := namespace.Clusters[cluster]
	if !ok {
		return nil, metadata.NewError("cluster", metadata.CodeNoExists, "")
	}
	if shardIdx >= len(c.Shards) || shardIdx < 0 {
		return nil, metadata.ErrShardIndexOutOfRange
	}
	s := c.Shards[shardIdx]
	nodes := make([]metadata.NodeInfo, 0, len(s.Nodes))
	copy(nodes, s.Nodes)
	return nodes, nil
}

func (memStorage *MemStorage) CreateNode(ns, cluster string, shardIdx int, node *metadata.NodeInfo) error {
	memStorage.mu.Lock()
	defer memStorage.mu.Unlock()

	namespace, ok := memStorage.namespaces[ns]
	if !ok {
		return metadata.ErrNamespaceNoExists
	}
	c, ok := namespace.Clusters[cluster]
	if !ok {
		return metadata.ErrClusterNoExists
	}
	if shardIdx >= len(c.Shards) || shardIdx < 0 {
		return metadata.ErrShardIndexOutOfRange
	}
	s := c.Shards[shardIdx]
	if s.Nodes == nil {
		s.Nodes = make([]metadata.NodeInfo, 0)
	}
	for _, existedNode := range s.Nodes {
		if existedNode.Address == node.Address {
			return metadata.NewError("existedNode", metadata.CodeExisted, "")
		}
	}
	if len(s.Nodes) == 0 && !node.IsMaster() {
		return errors.New("you MUST add master node first")
	}
	if len(s.Nodes) != 0 && node.IsMaster() {
		return errors.New("the master node has already added in this shard")
	}
	// TODO: send the slaveof command if necessary
	s.Nodes = append(s.Nodes, *node)
	c.Shards[shardIdx] = s
	memStorage.emitEvent(storage.Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     shardIdx,
		NodeID:    node.ID,
		Type:      storage.EventNode,
		Command:   storage.CommandCreate,
	})
	return nil
}

func (memStorage *MemStorage) RemoveNode(ns, cluster string, shardIdx int, nodeID string) error {
	memStorage.mu.Lock()
	defer memStorage.mu.Unlock()

	namespace, ok := memStorage.namespaces[ns]
	if !ok {
		return metadata.ErrNamespaceNoExists
	}
	c, ok := namespace.Clusters[cluster]
	if !ok {
		return metadata.ErrClusterNoExists
	}
	if shardIdx >= len(c.Shards) || shardIdx < 0 {
		return metadata.ErrShardIndexOutOfRange
	}
	s := c.Shards[shardIdx]
	if s.Nodes == nil {
		return metadata.NewError("node", metadata.CodeNoExists, "")
	}
	nodeIdx := -1
	for idx, node := range s.Nodes {
		if node.ID == nodeID {
			nodeIdx = idx
			break
		}
	}
	if nodeIdx == -1 {
		return metadata.NewError("node", metadata.CodeNoExists, "")
	}
	node := s.Nodes[nodeIdx]
	if len(s.SlotRanges) != 0 {
		if len(s.Nodes) == 1 || node.IsMaster() {
			return errors.New("still some slots in this shard, please migrate them first")
		}
	} else {
		if node.IsMaster() && len(s.Nodes) > 1 {
			return errors.New("please remove slave Shards first")
		}
	}
	s.Nodes = append(s.Nodes[:nodeIdx], s.Nodes[nodeIdx+1:]...)
	c.Shards[shardIdx] = s
	memStorage.emitEvent(storage.Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     shardIdx,
		NodeID:    node.ID,
		Type:      storage.EventNode,
		Command:   storage.CommandRemove,
	})
	return nil
}

func (memStorage *MemStorage) UpdateNode(ns, cluster string, shardIdx int, node metadata.NodeInfo) error {
	memStorage.mu.Lock()
	defer memStorage.mu.Unlock()

	namespace, ok := memStorage.namespaces[ns]
	if !ok {
		return metadata.ErrNamespaceNoExists
	}
	c, ok := namespace.Clusters[cluster]
	if !ok {
		return metadata.NewError("cluster", metadata.CodeNoExists, "")
	}
	if shardIdx >= len(c.Shards) || shardIdx < 0 {
		return metadata.ErrShardIndexOutOfRange
	}
	s := c.Shards[shardIdx]
	if s.Nodes == nil {
		return metadata.ErrNodeNoExists
	}
	// TODO: check the role
	nodeIdx := -1
	for idx, existedNode := range s.Nodes {
		if existedNode.ID == node.ID {
			memStorage.emitEvent(storage.Event{
				Namespace: ns,
				Cluster:   cluster,
				Shard:     shardIdx,
				NodeID:    node.ID,
				Type:      storage.EventNode,
				Command:   storage.CommandUpdate,
			})
			s.Nodes[idx] = node
			c.Shards[shardIdx] = s
			nodeIdx = idx
			break
		}
	}
	if nodeIdx == -1 {
		return metadata.NewError("node", metadata.CodeNoExists, "")
	}
	return nil
}
