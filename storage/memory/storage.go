package memory

import (
	"encoding/json"
	"errors"
	"sync"

	"github.com/KvrocksLabs/kvrocks-controller/storage"

	"github.com/KvrocksLabs/kvrocks-controller/metadata"
)

type Namespace struct {
	Clusters map[string]*Cluster
}

type Shard struct {
	nodes         map[string]*metadata.NodeInfo
	slotRanges    []metadata.SlotRange
	importingSlot int
	migratingSlot int
}

func (shard Shard) MarshalJSON() ([]byte, error) {
	var tmp struct {
		Nodes         []string             `json:"nodes"`
		SlotRanges    []metadata.SlotRange `json:"slot_ranges"`
		ImportingSlot int                  `json:"importing_slot"`
		MigratingSlot int                  `json:"migrating_slot"`
	}
	for _, node := range shard.nodes {
		tmp.Nodes = append(tmp.Nodes, node.ID)
	}
	tmp.SlotRanges = shard.slotRanges
	tmp.ImportingSlot = shard.importingSlot
	tmp.MigratingSlot = shard.migratingSlot
	return json.Marshal(tmp)
}

type Cluster struct {
	shards map[string]*Shard
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
		eventNotifyCh: make(chan storage.Event, 0),
		leaderElectCh: make(chan struct{}, 0),
	}
}

func (memStorage *MemStorage) Open() error {
	return nil
}

func (memStorage *MemStorage) BecomeLeader(id string) (bool, <-chan struct{}) {
	return true, memStorage.leaderElectCh
}

func (memStorage *MemStorage) Notify() chan<- storage.Event {
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
		return metadata.NewError("namespace", metadata.CodeExisted, "")
	}
	memStorage.namespaces[name] = &Namespace{
		Clusters: make(map[string]*Cluster),
	}
	return nil
}

func (memStorage *MemStorage) RemoveNamespace(name string) error {
	memStorage.mu.Lock()
	defer memStorage.mu.Unlock()
	if namespace, ok := memStorage.namespaces[name]; ok && namespace != nil {
		if len(namespace.Clusters) != 0 {
			return errors.New("namespace wasn't empty, please remove clusters first")
		}
		delete(memStorage.namespaces, name)
		return nil
	}
	return metadata.NewError("namespace", metadata.CodeNoExists, "")
}

func (memStorage *MemStorage) ListCluster(namespace string) ([]string, error) {
	memStorage.mu.RLock()
	defer memStorage.mu.RUnlock()
	ns, ok := memStorage.namespaces[namespace]
	if !ok {
		return nil, metadata.NewError("namespace", metadata.CodeNoExists, "")
	}

	clusterNames := make([]string, 0, len(ns.Clusters))
	for name := range ns.Clusters {
		clusterNames = append(clusterNames, name)
	}
	return clusterNames, nil
}

func (memStorage *MemStorage) CreateCluster(ns, name string) error {
	memStorage.mu.Lock()
	defer memStorage.mu.Unlock()
	if namespace, ok := memStorage.namespaces[ns]; ok {
		if namespace.Clusters == nil {
			memStorage.namespaces[ns].Clusters = make(map[string]*Cluster)
		}
		if _, ok := namespace.Clusters[name]; ok {
			return metadata.NewError("cluster", metadata.CodeExisted, "")
		}
		memStorage.namespaces[ns].Clusters[name] = &Cluster{
			shards: make(map[string]*Shard),
		}
		memStorage.emitEvent(storage.Event{
			Namespace: ns,
			Cluster:   name,
			Type:      storage.EventCluster,
			Command:   storage.CommandCreate,
		})
		return nil
	}
	return metadata.NewError("namespace", metadata.CodeNoExists, "")
}

func (memStorage *MemStorage) GetCluster(ns, name string) (*Cluster, error) {
	memStorage.mu.RLock()
	defer memStorage.mu.RUnlock()
	if namespace, ok := memStorage.namespaces[ns]; ok {
		if namespace.Clusters == nil {
			return nil, metadata.NewError("cluster", metadata.CodeNoExists, "")
		}
		if cluster, ok := namespace.Clusters[name]; !ok {
			return nil, metadata.NewError("cluster", metadata.CodeNoExists, "")
		} else {
			return cluster, nil
		}
	}
	return nil, metadata.NewError("namespace", metadata.CodeNoExists, "")
}

func (memStorage *MemStorage) RemoveCluster(ns, name string) error {
	memStorage.mu.Lock()
	defer memStorage.mu.Unlock()
	if namespace, ok := memStorage.namespaces[ns]; ok {
		if namespace.Clusters == nil {
			return metadata.NewError("cluster", metadata.CodeNoExists, "")
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
		return metadata.NewError("cluster", metadata.CodeNoExists, "")
	}
	return metadata.NewError("namespace", metadata.CodeNoExists, "")
}

func (memStorage *MemStorage) AddShardSlots(ns, cluster, shard string, slotRanges []metadata.SlotRange) error {
	c, err := memStorage.GetCluster(ns, cluster)
	if err != nil {
		return err
	}
	for _, slotRange := range slotRanges {
		if err := c.checkOverlap(&slotRange); err != nil {
			return err
		}
	}
	s, err := memStorage.GetShard(ns, cluster, shard)
	if err != nil {
		return err
	}
	if len(s.nodes) == 0 {
		return errors.New("the shard was empty, please add nodes first")
	}
	// TODO: merge slot ranges
	s.slotRanges = append(s.slotRanges, slotRanges...)
	memStorage.emitEvent(storage.Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     shard,
		Type:      storage.EventShard,
		Command:   storage.CommandAddSlots,
	})
	return nil
}

func (memStorage *MemStorage) ListShard(ns, cluster string) ([]string, error) {
	memStorage.mu.RLock()
	defer memStorage.mu.RUnlock()

	namespace, ok := memStorage.namespaces[ns]
	if !ok {
		return nil, metadata.NewError("namespace", metadata.CodeNoExists, "")
	}
	c, ok := namespace.Clusters[cluster]
	if !ok {
		return nil, metadata.NewError("cluster", metadata.CodeNoExists, "")
	}
	shardNames := make([]string, 0, len(c.shards))
	for name := range c.shards {
		shardNames = append(shardNames, name)
	}
	return shardNames, nil
}

func (memStorage *MemStorage) CreateShard(ns, cluster, name string) error {
	memStorage.mu.Lock()
	defer memStorage.mu.Unlock()

	namespace, ok := memStorage.namespaces[ns]
	if !ok {
		return metadata.NewError("namespace", metadata.CodeNoExists, "")
	}
	c, ok := namespace.Clusters[cluster]
	if !ok {
		return metadata.NewError("cluster", metadata.CodeNoExists, "")
	}
	if c.shards == nil {
		c.shards = make(map[string]*Shard)
	}
	if _, ok := c.shards[name]; ok {
		return metadata.NewError("shard", metadata.CodeExisted, "")
	}
	c.shards[name] = &Shard{}
	memStorage.emitEvent(storage.Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     name,
		Type:      storage.EventShard,
		Command:   storage.CommandCreate,
	})
	return nil
}

func (memStorage *MemStorage) GetShard(ns, cluster, name string) (*Shard, error) {
	memStorage.mu.RLock()
	defer memStorage.mu.RUnlock()

	namespace, ok := memStorage.namespaces[ns]
	if !ok {
		return nil, metadata.NewError("namespace", metadata.CodeNoExists, "")
	}
	c, ok := namespace.Clusters[cluster]
	if !ok {
		return nil, metadata.NewError("cluster", metadata.CodeNoExists, "")
	}
	if c.shards == nil {
		return nil, metadata.NewError("shard", metadata.CodeNoExists, "")
	}
	if shard, ok := c.shards[name]; !ok {
		return nil, metadata.NewError("shard", metadata.CodeNoExists, "")
	} else {
		return shard, nil
	}
}

func (memStorage *MemStorage) RemoveShard(ns, cluster, name string) error {
	memStorage.mu.Lock()
	defer memStorage.mu.Unlock()

	namespace, ok := memStorage.namespaces[ns]
	if !ok {
		return metadata.NewError("namespace", metadata.CodeNoExists, "")
	}
	c, ok := namespace.Clusters[cluster]
	if !ok {
		return metadata.NewError("cluster", metadata.CodeNoExists, "")
	}
	if _, ok := c.shards[name]; ok {
		delete(c.shards, name)
		memStorage.emitEvent(storage.Event{
			Namespace: ns,
			Cluster:   cluster,
			Shard:     name,
			Type:      storage.EventShard,
			Command:   storage.CommandRemove,
		})
		return nil
	}
	return metadata.NewError("shard", metadata.CodeNoExists, "")
}

func (memStorage *MemStorage) MigrateSlot(ns, cluster, source, target string, slot int) error {
	return nil
}

func (memStorage *MemStorage) ListNodes(ns, cluster, shard string) ([]metadata.NodeInfo, error) {
	memStorage.mu.RLock()
	defer memStorage.mu.RUnlock()

	namespace, ok := memStorage.namespaces[ns]
	if !ok {
		return nil, metadata.NewError("namespace", metadata.CodeNoExists, "")
	}
	c, ok := namespace.Clusters[cluster]
	if !ok {
		return nil, metadata.NewError("cluster", metadata.CodeNoExists, "")
	}
	s, ok := c.shards[shard]
	if !ok {
		return nil, metadata.NewError("shard", metadata.CodeNoExists, "")
	}
	nodes := make([]metadata.NodeInfo, 0, len(s.nodes))
	for _, node := range s.nodes {
		nodes = append(nodes, *node)
	}
	return nodes, nil
}

func (memStorage *MemStorage) CreateNode(ns, cluster, shard string, node *metadata.NodeInfo) error {
	memStorage.mu.Lock()
	defer memStorage.mu.Unlock()

	namespace, ok := memStorage.namespaces[ns]
	if !ok {
		return metadata.NewError("namespace", metadata.CodeNoExists, "")
	}
	c, ok := namespace.Clusters[cluster]
	if !ok {
		return metadata.NewError("cluster", metadata.CodeNoExists, "")
	}
	s, ok := c.shards[shard]
	if !ok {
		return metadata.NewError("shard", metadata.CodeNoExists, "")
	}
	if s.nodes == nil {
		s.nodes = make(map[string]*metadata.NodeInfo)
	}
	if _, ok := s.nodes[node.ID]; ok {
		return metadata.NewError("node", metadata.CodeExisted, "")
	}
	if len(s.nodes) == 0 && !node.IsMaster() {
		return errors.New("you MUST add master node first")
	}
	if len(s.nodes) != 0 && node.IsMaster() {
		return errors.New("the master node has already added in this shard")
	}
	// TODO: send the slaveof command if necessary
	s.nodes[node.ID] = node
	memStorage.emitEvent(storage.Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     shard,
		NodeID:    node.ID,
		Type:      storage.EventNode,
		Command:   storage.CommandCreate,
	})
	return nil
}

func (memStorage *MemStorage) RemoveNode(ns, cluster, shard string, nodeID string) error {
	memStorage.mu.Lock()
	defer memStorage.mu.Unlock()

	namespace, ok := memStorage.namespaces[ns]
	if !ok {
		return metadata.NewError("namespace", metadata.CodeNoExists, "")
	}
	c, ok := namespace.Clusters[cluster]
	if !ok {
		return metadata.NewError("cluster", metadata.CodeNoExists, "")
	}
	s, ok := c.shards[shard]
	if !ok {
		return metadata.NewError("shard", metadata.CodeNoExists, "")
	}
	if s.nodes == nil {
		return metadata.NewError("node", metadata.CodeNoExists, "")
	}
	node, ok := s.nodes[nodeID]
	if !ok {
		return metadata.NewError("node", metadata.CodeNoExists, "")
	}
	if len(s.slotRanges) != 0 {
		if len(s.nodes) == 1 || node.IsMaster() {
			return errors.New("still some slots in this shard, please migrate them first")
		}
	} else {
		if node.IsMaster() && len(s.nodes) > 1 {
			return errors.New("please remove slave nodes first")
		}
	}
	delete(s.nodes, nodeID)
	memStorage.emitEvent(storage.Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     shard,
		NodeID:    node.ID,
		Type:      storage.EventNode,
		Command:   storage.CommandRemove,
	})
	return nil
}

func (memStorage *MemStorage) UpdateNode(ns, cluster, shard string, node *metadata.NodeInfo) error {
	memStorage.mu.Lock()
	defer memStorage.mu.Unlock()

	namespace, ok := memStorage.namespaces[ns]
	if !ok {
		return metadata.NewError("namespace", metadata.CodeNoExists, "")
	}
	c, ok := namespace.Clusters[cluster]
	if !ok {
		return metadata.NewError("cluster", metadata.CodeNoExists, "")
	}
	s, ok := c.shards[shard]
	if !ok {
		return metadata.NewError("shard", metadata.CodeNoExists, "")
	}
	if s.nodes == nil {
		return metadata.NewError("node", metadata.CodeNoExists, "")
	}
	// TODO: check the role
	if _, ok := s.nodes[node.ID]; ok {
		memStorage.emitEvent(storage.Event{
			Namespace: ns,
			Cluster:   cluster,
			Shard:     shard,
			NodeID:    node.ID,
			Type:      storage.EventNode,
			Command:   storage.CommandUpdate,
		})
		s.nodes[node.ID] = node
		return nil
	}
	return metadata.NewError("node", metadata.CodeNoExists, "")
}

func (cluster *Cluster) checkOverlap(slotRange *metadata.SlotRange) error {
	for name, shard := range cluster.shards {
		if shard.HasOverlap(slotRange) {
			return errors.New("the slot range was owned by shard: " + name)
		}
	}
	return nil
}

func (shard *Shard) HasOverlap(slotRange *metadata.SlotRange) bool {
	for _, shardSlotRange := range shard.slotRanges {
		if shardSlotRange.HasOverlap(slotRange) {
			return true
		}
	}
	return false
}
