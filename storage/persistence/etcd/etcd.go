package etcd

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/KvrocksLabs/kvrocks_controller/logger"
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Etcd struct {
	client *clientv3.Client
	cli    clientv3.KV
}

func New(etcdAddrs []string) (*Etcd, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdAddrs,
		DialTimeout: defaultDailTimeout,
		Logger:      logger.Get(),
	})
	if err != nil {
		return nil, err
	}
	return &Etcd{
		client: client,
		cli:    clientv3.NewKV(client),
	}, nil
}

func (e *Etcd) Close() error {
	return e.client.Close()
}

func (e *Etcd) ListNamespace() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	resp, err := e.cli.Get(ctx, NamespaceKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var namespaces []string
	for _, kv := range resp.Kvs {
		if string(kv.Key) == NamespaceKeyPrefix {
			continue
		}
		namespaces = append(namespaces, string(kv.Value))
	}
	return namespaces, nil
}

func (e *Etcd) IsNamespaceExists(ns string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	resp, err := e.cli.Get(ctx, appendNamespacePrefix(ns))
	if err != nil {
		return false, err
	}
	return len(resp.Kvs) != 0, nil
}

// CreateNamespace add the specified namespace to storage
func (e *Etcd) CreateNamespace(ns string) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	_, err := e.cli.Put(ctx, appendNamespacePrefix(ns), ns)
	return err
}

// RemoveNamespace delete the specified namespace from storage
func (e *Etcd) RemoveNamespace(ns string) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	_, err := e.cli.Delete(ctx, appendNamespacePrefix(ns))
	return err
}

// ListCluster return the list of name of cluster under the specified namespace
func (e *Etcd) ListCluster(ns string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	clusterPrefix := buildClusterPrefix(ns)
	resp, err := e.cli.Get(ctx, clusterPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	prefixLen := len(clusterPrefix)
	var clusters []string
	for _, kv := range resp.Kvs {
		if string(kv.Key) == clusterPrefix {
			continue
		}
		cluster := string(kv.Key)
		clusters = append(clusters, cluster[prefixLen+1:])
	}
	return clusters, nil
}

func (e *Etcd) IsClusterExists(ns, cluster string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	resp, err := e.cli.Get(ctx, buildClusterKey(ns, cluster))
	if err != nil {
		return false, err
	}
	return len(resp.Kvs) != 0, nil
}

func (e *Etcd) GetClusterInfo(ns, cluster string) (metadata.Cluster, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	resp, err := e.cli.Get(ctx, buildClusterKey(ns, cluster))
	if err != nil {
		return metadata.Cluster{}, err
	}
	if len(resp.Kvs) == 0 {
		return metadata.Cluster{}, metadata.ErrClusterNoExists
	}
	var clusterInfo metadata.Cluster
	if err = json.Unmarshal(resp.Kvs[0].Value, &clusterInfo); err != nil {
		return metadata.Cluster{}, err
	}
	return clusterInfo, nil
}

func (e *Etcd) UpdateCluster(ns, cluster string, info *metadata.Cluster) error {
	if info == nil {
		return errors.New("nil cluster info")
	}
	clusterBytes, err := json.Marshal(info)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	_, err = e.cli.Put(ctx, buildClusterKey(ns, cluster), string(clusterBytes))
	return err
}

func (e *Etcd) CreateCluster(ns, cluster string, info *metadata.Cluster) error {
	return e.UpdateCluster(ns, cluster, info)
}

func (e *Etcd) RemoveCluster(ns, cluster string) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	if _, err := e.cli.Delete(ctx, buildClusterMetaKey(ns, cluster), clientv3.WithPrefix()); err != nil {
		return err
	}
	if _, err := e.cli.Delete(ctx, buildClusterKey(ns, cluster)); err != nil {
		return err
	}
	return nil
}
