package storage

import "github.com/KvrocksLabs/kvrocks-controller/metadata"

type Storage interface {
	Open() error
	BecomeLeader(id string) (bool, <-chan struct{})
	Notify() <-chan Event
	Close() error

	ListNamespace() ([]string, error)
	CreateNamespace(name string) error
	RemoveNamespace(name string) error

	ListCluster(namespace string) ([]string, error)
	GetCluster(namespace, cluster string) (*metadata.Cluster, error)
	CreateCluster(ns, name string, shards []metadata.Shard) error
	RemoveCluster(ns, name string) error

	AddShardSlots(ns, cluster string, shardIdx int, slotRanges []metadata.SlotRange) error
	RemoveShardSlots(ns, cluster string, shardIdx int, slotRanges []metadata.SlotRange) error
	ListShard(ns, cluster string) ([]metadata.Shard, error)
	CreateShard(ns, cluster string, shard *metadata.Shard) error
	GetShard(ns, cluster string, shardIdx int) (*metadata.Shard, error)
	RemoveShard(ns, cluster string, shardIdx int) error

	ListNodes(ns, cluster string, shardIdx int) ([]metadata.NodeInfo, error)
	CreateNode(ns, cluster string, shardIdx int, node *metadata.NodeInfo) error
	RemoveNode(ns, cluster string, shardIdx int, nodeID string) error
	UpdateNode(ns, cluster string, shardIdx int, node metadata.NodeInfo) error
}
