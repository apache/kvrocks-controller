package memory

import (
	"encoding/json"
	"errors"
	"sync"

	"github.com/KvrocksLabs/kvrocks-controller/meta"
)

type Namespace struct {
	Clusters map[string]*Cluster
}

type Shard struct {
	nodes         map[string]*meta.NodeInfo
	slotRanges    []meta.SlotRange
	importingSlot int
	migratingSlot int
}

func (shard Shard) MarshalJSON() ([]byte, error) {
	var tmp struct {
		Nodes         []string         `json:"nodes"`
		SlotRanges    []meta.SlotRange `json:"slot_ranges"`
		ImportingSlot int              `json:"importing_slot"`
		MigratingSlot int              `json:"migrating_slot"`
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
}

func NewMemStorage() *MemStorage {
	return &MemStorage{
		namespaces: make(map[string]*Namespace),
	}
}

func (storage *MemStorage) ListNamespace() ([]string, error) {
	storage.mu.RLock()
	defer storage.mu.RUnlock()
	namespaces := make([]string, 0, len(storage.namespaces))
	for name := range storage.namespaces {
		namespaces = append(namespaces, name)
	}
	return namespaces, nil
}

func (storage *MemStorage) CreateNamespace(name string) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	if namespace, ok := storage.namespaces[name]; ok && namespace != nil {
		return meta.NewError("namespace", meta.CodeExisted, "")
	}
	storage.namespaces[name] = &Namespace{
		Clusters: make(map[string]*Cluster),
	}
	return nil
}

func (storage *MemStorage) RemoveNamespace(name string) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	if namespace, ok := storage.namespaces[name]; ok && namespace != nil {
		if len(namespace.Clusters) != 0 {
			return errors.New("namespace wasn't empty, please remove clusters first")
		}
		delete(storage.namespaces, name)
		return nil
	}
	return meta.NewError("namespace", meta.CodeNoExists, "")
}

func (storage *MemStorage) ListCluster(namespace string) ([]string, error) {
	storage.mu.RLock()
	defer storage.mu.RUnlock()
	ns, ok := storage.namespaces[namespace]
	if !ok {
		return nil, meta.NewError("namespace", meta.CodeNoExists, "")
	}

	clusterNames := make([]string, 0, len(ns.Clusters))
	for name := range ns.Clusters {
		clusterNames = append(clusterNames, name)
	}
	return clusterNames, nil
}

func (storage *MemStorage) CreateCluster(ns, name string) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	if namespace, ok := storage.namespaces[ns]; ok {
		if namespace.Clusters == nil {
			storage.namespaces[ns].Clusters = make(map[string]*Cluster)
		}
		if _, ok := namespace.Clusters[name]; ok {
			return meta.NewError("cluster", meta.CodeExisted, "")
		}
		storage.namespaces[ns].Clusters[name] = &Cluster{
			shards: make(map[string]*Shard),
		}
		return nil
	}
	return meta.NewError("namespace", meta.CodeNoExists, "")
}

func (storage *MemStorage) GetCluster(ns, name string) (*Cluster, error) {
	storage.mu.RLock()
	defer storage.mu.RUnlock()
	if namespace, ok := storage.namespaces[ns]; ok {
		if namespace.Clusters == nil {
			return nil, meta.NewError("cluster", meta.CodeNoExists, "")
		}
		if cluster, ok := namespace.Clusters[name]; !ok {
			return nil, meta.NewError("cluster", meta.CodeNoExists, "")
		} else {
			return cluster, nil
		}
	}
	return nil, meta.NewError("namespace", meta.CodeNoExists, "")
}

func (storage *MemStorage) RemoveCluster(ns, name string) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	if namespace, ok := storage.namespaces[ns]; ok {
		if namespace.Clusters == nil {
			return meta.NewError("cluster", meta.CodeNoExists, "")
		}
		if _, ok := namespace.Clusters[name]; ok {
			delete(storage.namespaces[ns].Clusters, name)
			return nil
		}
		return meta.NewError("cluster", meta.CodeNoExists, "")
	}
	return meta.NewError("namespace", meta.CodeNoExists, "")
}

func (storage *MemStorage) AddShardSlots(ns, cluster, shard string, slotRanges []meta.SlotRange) error {
	c, err := storage.GetCluster(ns, cluster)
	if err != nil {
		return err
	}
	for _, slotRange := range slotRanges {
		if err := c.checkOverlap(&slotRange); err != nil {
			return err
		}
	}
	s, err := storage.GetShard(ns, cluster, shard)
	if err != nil {
		return err
	}
	if len(s.nodes) == 0 {
		return errors.New("the shard was empty, please add nodes first")
	}
	// TODO: merge slot ranges
	s.slotRanges = append(s.slotRanges, slotRanges...)
	return nil
}

func (storage *MemStorage) ListShard(ns, cluster string) ([]string, error) {
	storage.mu.RLock()
	defer storage.mu.RUnlock()

	namespace, ok := storage.namespaces[ns]
	if !ok {
		return nil, meta.NewError("namespace", meta.CodeNoExists, "")
	}
	c, ok := namespace.Clusters[cluster]
	if !ok {
		return nil, meta.NewError("cluster", meta.CodeNoExists, "")
	}
	shardNames := make([]string, 0, len(c.shards))
	for name := range c.shards {
		shardNames = append(shardNames, name)
	}
	return shardNames, nil
}

func (storage *MemStorage) CreateShard(ns, cluster, name string) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	namespace, ok := storage.namespaces[ns]
	if !ok {
		return meta.NewError("namespace", meta.CodeNoExists, "")
	}
	c, ok := namespace.Clusters[cluster]
	if !ok {
		return meta.NewError("cluster", meta.CodeNoExists, "")
	}
	if c.shards == nil {
		c.shards = make(map[string]*Shard)
	}
	if _, ok := c.shards[name]; ok {
		return meta.NewError("shard", meta.CodeExisted, "")
	}
	c.shards[name] = &Shard{}
	return nil
}

func (storage *MemStorage) GetShard(ns, cluster, name string) (*Shard, error) {
	storage.mu.RLock()
	defer storage.mu.RUnlock()

	namespace, ok := storage.namespaces[ns]
	if !ok {
		return nil, meta.NewError("namespace", meta.CodeNoExists, "")
	}
	c, ok := namespace.Clusters[cluster]
	if !ok {
		return nil, meta.NewError("cluster", meta.CodeNoExists, "")
	}
	if c.shards == nil {
		return nil, meta.NewError("shard", meta.CodeNoExists, "")
	}
	if shard, ok := c.shards[name]; !ok {
		return nil, meta.NewError("shard", meta.CodeNoExists, "")
	} else {
		return shard, nil
	}
}

func (storage *MemStorage) RemoveShard(ns, cluster, name string) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	namespace, ok := storage.namespaces[ns]
	if !ok {
		return meta.NewError("namespace", meta.CodeNoExists, "")
	}
	c, ok := namespace.Clusters[cluster]
	if !ok {
		return meta.NewError("cluster", meta.CodeNoExists, "")
	}
	if _, ok := c.shards[name]; ok {
		delete(c.shards, name)
		return nil
	}
	return meta.NewError("shard", meta.CodeNoExists, "")
}

func (storage *MemStorage) MigrateSlot(ns, cluster, source, target string, slot int) error {
	return nil
}

func (storage *MemStorage) ListNodes(ns, cluster, shard string) ([]meta.NodeInfo, error) {
	storage.mu.RLock()
	defer storage.mu.RUnlock()

	namespace, ok := storage.namespaces[ns]
	if !ok {
		return nil, meta.NewError("namespace", meta.CodeNoExists, "")
	}
	c, ok := namespace.Clusters[cluster]
	if !ok {
		return nil, meta.NewError("cluster", meta.CodeNoExists, "")
	}
	s, ok := c.shards[shard]
	if !ok {
		return nil, meta.NewError("shard", meta.CodeNoExists, "")
	}
	nodes := make([]meta.NodeInfo, 0, len(s.nodes))
	for _, node := range s.nodes {
		nodes = append(nodes, *node)
	}
	return nodes, nil
}

func (storage *MemStorage) CreateNode(ns, cluster, shard string, node *meta.NodeInfo) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	namespace, ok := storage.namespaces[ns]
	if !ok {
		return meta.NewError("namespace", meta.CodeNoExists, "")
	}
	c, ok := namespace.Clusters[cluster]
	if !ok {
		return meta.NewError("cluster", meta.CodeNoExists, "")
	}
	s, ok := c.shards[shard]
	if !ok {
		return meta.NewError("shard", meta.CodeNoExists, "")
	}
	if s.nodes == nil {
		s.nodes = make(map[string]*meta.NodeInfo)
	}
	if _, ok := s.nodes[node.ID]; ok {
		return meta.NewError("node", meta.CodeExisted, "")
	}
	if len(s.nodes) == 0 && !node.IsMaster() {
		return errors.New("you MUST add master node first")
	}
	if len(s.nodes) != 0 && node.IsMaster() {
		return errors.New("the master node has already added in this shard")
	}
	// TODO: send the slaveof command if necessary
	s.nodes[node.ID] = node
	return nil
}

func (storage *MemStorage) RemoveNode(ns, cluster, shard string, nodeID string) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	namespace, ok := storage.namespaces[ns]
	if !ok {
		return meta.NewError("namespace", meta.CodeNoExists, "")
	}
	c, ok := namespace.Clusters[cluster]
	if !ok {
		return meta.NewError("cluster", meta.CodeNoExists, "")
	}
	s, ok := c.shards[shard]
	if !ok {
		return meta.NewError("shard", meta.CodeNoExists, "")
	}
	if s.nodes == nil {
		return meta.NewError("node", meta.CodeNoExists, "")
	}
	node, ok := s.nodes[nodeID]
	if !ok {
		return meta.NewError("node", meta.CodeNoExists, "")
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
	return nil
}

func (storage *MemStorage) UpdateNode(ns, cluster, shard string, node *meta.NodeInfo) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	namespace, ok := storage.namespaces[ns]
	if !ok {
		return meta.NewError("namespace", meta.CodeNoExists, "")
	}
	c, ok := namespace.Clusters[cluster]
	if !ok {
		return meta.NewError("cluster", meta.CodeNoExists, "")
	}
	s, ok := c.shards[shard]
	if !ok {
		return meta.NewError("shard", meta.CodeNoExists, "")
	}
	if s.nodes == nil {
		return meta.NewError("node", meta.CodeNoExists, "")
	}
	// TODO: check the role
	if _, ok := s.nodes[node.ID]; ok {
		s.nodes[node.ID] = node
		return nil
	}
	return meta.NewError("node", meta.CodeNoExists, "")
}

func (cluster *Cluster) checkOverlap(slotRange *meta.SlotRange) error {
	for name, shard := range cluster.shards {
		if shard.HasOverlap(slotRange) {
			return errors.New("the slot range was owned by shard: " + name)
		}
	}
	return nil
}

func (shard *Shard) HasOverlap(slotRange *meta.SlotRange) bool {
	for _, shardSlotRange := range shard.slotRanges {
		if shardSlotRange.HasOverlap(slotRange) {
			return true
		}
	}
	return false
}
