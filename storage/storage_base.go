package storage

import (
	"errors"

	"github.com/KvrocksLabs/kvrocks-controller/metadata"
)

// ListNamespace return the list of name of all namespaces
func (stor *Storage) ListNamespace() ([]string, error) {
	stor.rw.RLock()
	defer stor.rw.RUnlock()
	if !stor.selfLeaderWithUnLock() {
		return nil, ErrSlaveNoSupport
	}
	return stor.local.ListNamespace()
}

// HasNamespace return an indicator whether the specified namespace exists
func (stor *Storage) HasNamespace(ns string) (bool, error) {
	stor.rw.RLock()
	defer stor.rw.RUnlock()
	if !stor.selfLeaderWithUnLock() {
		return false, ErrSlaveNoSupport
	}
	return stor.local.HasNamespace(ns)
}

// CreateNamespace add the specified namespace to storage 
func (stor *Storage) CreateNamespace(ns string) error {
	stor.rw.Lock()
	defer stor.rw.Unlock()
	if !stor.selfLeaderWithUnLock() {
		return ErrSlaveNoSupport
	}
	if has, _ := stor.local.HasNamespace(ns); has {
		return metadata.ErrNamespaceHasExisted
	}
	if err := stor.remote.CreateNamespace(ns); err != nil {
		return err
	}
	stor.local.CreateNamespace(ns)
	stor.EmitEvent(Event{
		Namespace: ns,
		Type:      EventNamespace,
		Command:   CommandCreate,
	})
	return nil
}

// RemoveNamespace delete the specified namespace from storage 
func (stor *Storage) RemoveNamespace(ns string) error {
	stor.rw.Lock()
	defer stor.rw.Unlock()
	if !stor.selfLeaderWithUnLock() {
		return ErrSlaveNoSupport
	}
	if has, _ := stor.local.HasNamespace(ns); !has {
		return metadata.ErrNamespaceNoExists
	}
	clusters, err := stor.local.ListCluster(ns)
	if err != nil {
		return err
	}
	if len(clusters) != 0 {
		return errors.New("namespace wasn't empty, please remove clusters first")
	}
	if err := stor.remote.RemoveNamespace(ns); err != nil {
		return err
	}
	stor.local.RemoveNamespace(ns)
	stor.EmitEvent(Event{
		Namespace: ns,
		Type:      EventNamespace,
		Command:   CommandRemove,
	})
	return nil
}

// ListCluster return the list of name of cluster under the specified namespace
func (stor *Storage) ListCluster(ns string) ([]string, error) {
	stor.rw.RLock()
	defer stor.rw.RUnlock()
	if !stor.selfLeaderWithUnLock() {
		return nil, ErrSlaveNoSupport
	}
	return stor.local.ListCluster(ns)
}

// HasCluster return an indicator whether the cluster under the specified namespace
func (stor *Storage) HasCluster(ns, cluster string) (bool, error) {
	stor.rw.RLock()
	defer stor.rw.RUnlock()
	if !stor.selfLeaderWithUnLock() {
		return false, ErrSlaveNoSupport
	}
	return stor.local.HasCluster(ns, cluster)
}

// GetClusterCopy return a copy of specified 'metadata.Cluster' under the specified namespace
func (stor *Storage) GetClusterCopy(ns, cluster string) (metadata.Cluster, error) {
	stor.rw.RLock()
	defer stor.rw.RUnlock()
	if !stor.selfLeaderWithUnLock() {
		return metadata.Cluster{}, ErrSlaveNoSupport
	}
	return stor.local.GetClusterCopy(ns, cluster)
}

// UpdateCluster update the Cluster to storage under the specified namespace
func (stor *Storage) UpdateCluster(ns, cluster string, topo *metadata.Cluster) error {
	stor.rw.Lock()
	defer stor.rw.Unlock()
	if !stor.selfLeaderWithUnLock() {
		return ErrSlaveNoSupport
	}
	return stor.updateCluster(ns, cluster, topo)
}

// updateCluster is goroutine unsafety of UpdateCluster
// assumption caller has hold the lock
func (stor *Storage) updateCluster(ns, cluster string, topo *metadata.Cluster) error {
	if has, _ := stor.local.HasNamespace(ns); !has {
		return metadata.ErrNamespaceNoExists
	}
	if len(topo.Shards) == 0 {
		return errors.New("required at least one shard")
	}
	if err := stor.remote.UpdateCluster(ns, cluster, topo); err != nil {
		return err
	}
	stor.local.UpdateCluster(ns, cluster, topo)
	return nil
}

// CreateCluster add a Cluster to storage under the specified namespace
func (stor *Storage) CreateCluster(ns, cluster string, topo *metadata.Cluster) error {
	stor.rw.Lock()
	defer stor.rw.Unlock()
	if !stor.selfLeaderWithUnLock() {
		return ErrSlaveNoSupport
	}
	if has, _ := stor.local.HasCluster(ns, cluster); has {
		return metadata.ErrClusterHasExisted
	}
	if err := stor.updateCluster(ns, cluster, topo); err != nil {
		return err
	}
	stor.EmitEvent(Event{
		Namespace: ns,
		Cluster:   cluster,
		Type:      EventCluster,
		Command:   CommandCreate,
	})
	return nil
}

// RemoveCluster delete the Cluster from storage under the specified namespace
func (stor *Storage) RemoveCluster(ns, cluster string) error {
	stor.rw.Lock()
	defer stor.rw.Unlock()
	if !stor.selfLeaderWithUnLock() {
		return ErrSlaveNoSupport
	}
	if has, _ := stor.local.HasNamespace(ns); !has {
		return metadata.ErrNamespaceNoExists
	}
	if has, _ := stor.local.HasCluster(ns, cluster); !has {
		return metadata.ErrClusterNoExists
	}
	if err := stor.remote.RemoveCluster(ns, cluster); err != nil {
		return err
	}
	stor.local.RemoveCluster(ns, cluster)
	stor.EmitEvent(Event{
		Namespace: ns,
		Cluster:   cluster,
		Type:      EventCluster,
		Command:   CommandRemove,
	})
	return nil
}
