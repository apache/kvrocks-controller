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
	kv     clientv3.KV
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
		kv:     clientv3.NewKV(client),
	}, nil
}

func (e *Etcd) Close() error {
	return e.client.Close()
}

func (e *Etcd) ListNamespace(ctx context.Context) ([]string, error) {
	resp, err := e.kv.Get(ctx, NamespaceKeyPrefix, clientv3.WithPrefix())
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

func (e *Etcd) IsNamespaceExists(ctx context.Context, ns string) (bool, error) {
	resp, err := e.kv.Get(ctx, appendNamespacePrefix(ns))
	if err != nil {
		return false, err
	}
	return len(resp.Kvs) != 0, nil
}

func (e *Etcd) CreateNamespace(ctx context.Context, ns string) error {
	_, err := e.kv.Put(ctx, appendNamespacePrefix(ns), ns)
	return err
}

func (e *Etcd) RemoveNamespace(ns string, ctx context.Context) error {
	_, err := e.kv.Delete(ctx, appendNamespacePrefix(ns))
	return err
}

func (e *Etcd) ListCluster(ctx context.Context, ns string) ([]string, error) {
	clusterPrefix := buildClusterPrefix(ns)
	resp, err := e.kv.Get(ctx, clusterPrefix, clientv3.WithPrefix())
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

func (e *Etcd) IsClusterExists(ctx context.Context, ns, cluster string) (bool, error) {
	resp, err := e.kv.Get(ctx, buildClusterKey(ns, cluster))
	if err != nil {
		return false, err
	}
	return len(resp.Kvs) != 0, nil
}

func (e *Etcd) GetCluster(ctx context.Context, ns, cluster string) (metadata.Cluster, error) {
	resp, err := e.kv.Get(ctx, buildClusterKey(ns, cluster))
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

func (e *Etcd) UpdateCluster(ctx context.Context, ns, cluster string, info *metadata.Cluster) error {
	if info == nil {
		return errors.New("nil cluster info")
	}
	clusterBytes, err := json.Marshal(info)
	if err != nil {
		return err
	}
	_, err = e.kv.Put(ctx, buildClusterKey(ns, cluster), string(clusterBytes))
	return err
}

func (e *Etcd) CreateCluster(ctx context.Context, ns, cluster string, info *metadata.Cluster) error {
	return e.UpdateCluster(ctx, ns, cluster, info)
}

func (e *Etcd) RemoveCluster(ctx context.Context, ns, cluster string) error {
	if _, err := e.kv.Delete(ctx, buildClusterMetaKey(ns, cluster), clientv3.WithPrefix()); err != nil {
		return err
	}
	if _, err := e.kv.Delete(ctx, buildClusterKey(ns, cluster)); err != nil {
		return err
	}
	return nil
}
