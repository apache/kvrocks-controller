package memory

import (
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
)

// Namespace implement memory map of namespace to cluster
type Namespace struct {
	Clusters map[string]*metadata.Cluster
}

// MemStorage implement BaseStorage `interface`
type MemStorage struct {
	namespaces map[string]*Namespace
}

// NewMemStorage create memory map of topo data
func NewMemStorage() *MemStorage {
	return &MemStorage{
		namespaces: make(map[string]*Namespace),
	}
}

// ListNamespace return the list of name of Namespace
func (m *MemStorage) ListNamespace() ([]string, error) {
	namespaces := make([]string, 0, len(m.namespaces))
	for name := range m.namespaces {
		namespaces = append(namespaces, name)
	}
	return namespaces, nil
}

// HasNamespace return an indicator whether the specified namespace exists
func (m *MemStorage) HasNamespace(ns string) (bool, error) {
	_, ok := m.namespaces[ns]
	return ok, nil
}

// CreateNamespace add the specified namespace to storage
func (m *MemStorage) CreateNamespace(ns string) error {
	if namespace, ok := m.namespaces[ns]; ok && namespace != nil {
		return metadata.ErrNamespaceExisted
	}
	m.namespaces[ns] = &Namespace{
		Clusters: make(map[string]*metadata.Cluster),
	}
	return nil
}

// RemoveNamespace delete the specified namespace from storage
func (m *MemStorage) RemoveNamespace(ns string) error {
	if _, ok := m.namespaces[ns]; ok {
		delete(m.namespaces, ns)
		return nil
	}
	return metadata.ErrNamespaceNoExists
}

// ListCluster return the list of name of cluster under the specified namespace
func (m *MemStorage) ListCluster(ns string) ([]string, error) {
	namespace, ok := m.namespaces[ns]
	if !ok {
		return nil, metadata.ErrNamespaceNoExists
	}
	clusterNames := make([]string, 0, len(namespace.Clusters))
	for name := range namespace.Clusters {
		clusterNames = append(clusterNames, name)
	}
	return clusterNames, nil
}

// HasCluster return an indicator whether the cluster under the specified namespace
func (m *MemStorage) HasCluster(ns, cluster string) (bool, error) {
	if _, ok := m.namespaces[ns]; !ok {
		return false, metadata.ErrNamespaceNoExists
	}
	if _, ok := m.namespaces[ns].Clusters[cluster]; !ok {
		return false, metadata.ErrClusterNoExists
	}
	return true, nil
}

// GetClusterCopy return a copy of specified 'metadata.Cluster' under the specified namespace
func (m *MemStorage) GetClusterCopy(ns, cluster string) (metadata.Cluster, error) {
	namespace, ok := m.namespaces[ns]
	if !ok {
		return metadata.Cluster{}, metadata.ErrNamespaceNoExists
	}
	if topo, ok := namespace.Clusters[cluster]; ok {
		return *topo, nil
	}
	return metadata.Cluster{}, metadata.ErrClusterNoExists
}

// UpdateCluster update the Name to storage under the specified namespace
func (m *MemStorage) UpdateCluster(ns, cluster string, topo *metadata.Cluster) error {
	namespace, ok := m.namespaces[ns]
	if !ok {
		return metadata.ErrNamespaceNoExists
	}
	namespace.Clusters[cluster] = topo
	return nil
}

// CreateCluster add a Name to storage under the specified namespace
func (m *MemStorage) CreateCluster(ns, cluster string, topo *metadata.Cluster) error {
	return m.UpdateCluster(ns, cluster, topo)
}

// RemoveCluster delete the Name from storage under the specified namespace
func (m *MemStorage) RemoveCluster(ns, cluster string) error {
	namespace, ok := m.namespaces[ns]
	if !ok {
		return metadata.ErrNamespaceNoExists
	}
	if topo, ok := namespace.Clusters[cluster]; ok && topo != nil {
		delete(namespace.Clusters, cluster)
		return nil
	}
	return metadata.ErrClusterNoExists
}
