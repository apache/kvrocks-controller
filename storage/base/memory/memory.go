package memory

import (
	"github.com/KvrocksLabs/kvrocks-controller/metadata"
)

// Namespace implement memory map of namespace to cluster
type Namespace struct {
	Clusters map[string]*metadata.Cluster
}

// MemStorage implment BaseStorage `interface`
type MemStorage struct {
	namespaces map[string]*Namespace
}

// NewMemStorage create memory map of topo data 
func NewMemStorage() *MemStorage{
	return &MemStorage{
		namespaces:    make(map[string]*Namespace),
	}
}

// ListNamespace return the list of name of Namespace
func (stor *MemStorage) ListNamespace() ([]string, error) {
	namespaces := make([]string, 0, len(stor.namespaces))
	for name := range stor.namespaces {
		namespaces = append(namespaces, name)
	}
	return namespaces, nil
}

// HasNamespace return an indicator whether the specified namespace exists
func (stor *MemStorage) HasNamespace(ns string) (bool, error) {
	_, ok := stor.namespaces[ns]
	return ok, nil
}

// CreateNamespace add the specified namespace to storage 
func (stor *MemStorage) CreateNamespace(ns string) error {
	if namespace, ok := stor.namespaces[ns]; ok && namespace != nil {
		return metadata.ErrNamespaceHasExisted
	}
	stor.namespaces[ns] = &Namespace{
		Clusters: make(map[string]*metadata.Cluster),
	}
	return nil
}

// RemoveNamespace delete the specified namespace from storage 
func (stor *MemStorage) RemoveNamespace(ns string) error {
	if _, ok := stor.namespaces[ns]; ok {
		delete(stor.namespaces, ns)
		return nil
	}
	return metadata.ErrNamespaceNoExists
}

// ListCluster return the list of name of cluster under the specified namespace
func (stor *MemStorage) ListCluster(ns string) ([]string, error) {
	namespace, ok := stor.namespaces[ns]
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
func (stor *MemStorage) HasCluster(ns, cluster string) (bool, error) {
	if _, ok := stor.namespaces[ns]; !ok {
		return false, metadata.ErrNamespaceNoExists
	}
	if _, ok := stor.namespaces[ns].Clusters[cluster]; !ok {
		return false, metadata.ErrClusterNoExists
	}
	return true, nil
}

// GetClusterCopy return a copy of specified 'metadata.Cluster' under the specified namespace
func (stor *MemStorage) GetClusterCopy(ns, cluster string) (metadata.Cluster, error) {
	namespace, ok := stor.namespaces[ns]
	if !ok {
		return metadata.Cluster{}, metadata.ErrNamespaceNoExists
	}
	if topo, ok := namespace.Clusters[cluster]; ok {
		return *topo, nil
	}
	return metadata.Cluster{}, metadata.ErrClusterNoExists
}

// UpdateCluster update the Cluster to storage under the specified namespace
func (stor *MemStorage) UpdateCluster(ns, cluster string, topo *metadata.Cluster) error {
	namespace, ok := stor.namespaces[ns]
	if !ok {
		return metadata.ErrNamespaceNoExists
	}
	namespace.Clusters[cluster] = topo
	return nil
}

// CreateCluster add a Cluster to storage under the specified namespace
func (stor *MemStorage) CreateCluster(ns, cluster string, topo *metadata.Cluster) error {
	return stor.UpdateCluster(ns, cluster, topo)
}

// RemoveCluster delete the Cluster from storage under the specified namespace
func (stor *MemStorage) RemoveCluster(ns, cluster string) error {
	namespace, ok := stor.namespaces[ns]
	if !ok {
		return metadata.ErrNamespaceNoExists
	}
	if topo, ok := namespace.Clusters[cluster]; ok && topo != nil {
		delete(namespace.Clusters, cluster)
		return nil
	}
	return metadata.ErrClusterNoExists
}
