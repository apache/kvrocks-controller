package etcd

import (
	"context"
	"encoding/json"

	"go.etcd.io/etcd/client/v3"
	"github.com/KvrocksLabs/kvrocks-controller/metadata"
)

// BaseStorage implment BaseStorage `interface`
type EtcdStorage struct {
	cli clientv3.KV
}

// NewMemStorage create etcd storage of topo data
func NewEtcdStorage(client *clientv3.Client) *EtcdStorage {
	return &EtcdStorage{
		cli: clientv3.NewKV(client),
	}
}

// ListNamespace return the list of name of Namespace
func (stor *EtcdStorage) ListNamespace() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	resp, err := stor.cli.Get(ctx, NamespaceKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var ns []string
	for _, kv := range resp.Kvs {
		if string(kv.Key) == NamespaceKeyPrefix {
			continue
		}
		ns = append(ns, string(kv.Value))
	}
	return ns, nil
}

// HasNamespace return an indicator whether the specified namespace exists
func (stor *EtcdStorage) HasNamespace(ns string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	resp, err := stor.cli.Get(ctx, NamespaceKey(ns))
	if err != nil {
		return false, err
	}
	return len(resp.Kvs) != 0, nil
}

// CreateNamespace add the specified namespace to storage 
func (stor *EtcdStorage) CreateNamespace(ns string) error {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	_, err := stor.cli.Put(ctx, NamespaceKey(ns), ns)
	return err
}

// RemoveNamespace delete the specified namespace from storage 
func (stor *EtcdStorage) RemoveNamespace(ns string) error {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	_, err := stor.cli.Delete(ctx, NamespaceKey(ns), clientv3.WithPrefix())
	return err
}

// ListCluster return the list of name of cluster under the specified namespace
func (stor *EtcdStorage) ListCluster(ns string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	nsClusterPrefix := NsClusterPrefixKey(ns)
	resp, err := stor.cli.Get(ctx, nsClusterPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	prefixlen := len(nsClusterPrefix)
	var clusters []string
	for _, kv := range resp.Kvs {
		if string(kv.Key) == nsClusterPrefix {
			continue
		}
		cluster := string(kv.Key)
		clusters = append(clusters, cluster[prefixlen:])
	}
	return clusters, nil
}

// HasCluster return an indicator whether the cluster under the specified namespace
func (stor *EtcdStorage) HasCluster(ns, cluster string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	resp, err := stor.cli.Get(ctx, NsClusterKey(ns, cluster))
	if err != nil {
		return false, err
	}
	return len(resp.Kvs) != 0, nil
}

// GetClusterCopy return a copy of specified 'metadata.Cluster' under the specified namespace
func (stor *EtcdStorage) GetClusterCopy(ns, cluster string) (metadata.Cluster, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	resp, err := stor.cli.Get(ctx, NsClusterKey(ns, cluster))
	if err != nil {
		return metadata.Cluster{}, err
	}
	if len(resp.Kvs) == 0 {
		return metadata.Cluster{}, metadata.ErrClusterNoExists
	}
	clusterData := resp.Kvs[0].Value
	var topo metadata.Cluster
	if err = json.Unmarshal(clusterData, &topo); err != nil {
        return metadata.Cluster{}, err
    }
    return topo, nil
}

// UpdateCluster update the Cluster to storage under the specified namespace
func (stor *EtcdStorage) UpdateCluster(ns, cluster string, topo *metadata.Cluster) error {
	clusterData, err := json.Marshal(topo)
    if err != nil {
        return err
    }
    ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
    _, err = stor.cli.Put(ctx, NsClusterKey(ns, cluster), string(clusterData))
	return err
}

// CreateCluster add a Cluster to storage under the specified namespace
func (stor *EtcdStorage) CreateCluster(ns, cluster string, topo *metadata.Cluster) error {
	return stor.UpdateCluster(ns, cluster, topo)
}

// RemoveCluster delete the Cluster from storage under the specified namespace
func (stor *EtcdStorage) RemoveCluster(ns, cluster string) error {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	if _, err := stor.cli.Delete(ctx, NsClusterMetaKey(ns, cluster), clientv3.WithPrefix()); err != nil {
		return err
	}
	if _, err := stor.cli.Delete(ctx, NsClusterKey(ns, cluster)); err != nil {
		return err
	}
	return nil
}
