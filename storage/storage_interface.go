package storage

import (
	"io"

	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/storage/base/etcd"
)

// NamespaceStorage wraps the Namespace methods of a backing data store.
type NamespaceStorage interface {
	// ListNamespace return the list of name of all namespaces
	ListNamespace() ([]string, error)

	// HasNamespace return an indicator whether the specified namespace exists
	HasNamespace(ns string) (bool, error)

	// CreateNamespace add the specified namespace to storage
	CreateNamespace(ns string) error

	// RemoveNamespace delete the specified namespace from storage
	RemoveNamespace(ns string) error

	// LoadData load namespace and cluster from etcd when start or switch leader
	LoadTasks() error
}

// ClusterStorage wraps the Cluster methods of a backing data store.
type ClusterStorage interface {
	// ListCluster return the list of name of cluster under the specified namespace
	ListCluster(ns string) ([]string, error)

	// HasCluster return an indicator whether the cluster under the specified namespace
	HasCluster(ns, cluster string) (bool, error)

	// GetClusterCopy return a copy of specified 'metadata.Cluster' under the specified namespace
	GetClusterCopy(ns, cluster string) (metadata.Cluster, error)

	// CreateCluster add a Cluster to storage under the specified namespace
	CreateCluster(ns, cluster string, topo *metadata.Cluster) error

	// UpdateCluster update the Cluster to storage under the specified namespace
	UpdateCluster(ns, cluster string, topo *metadata.Cluster) error

	// RemoveCluster delete the Cluster from storage under the specified namespace
	RemoveCluster(ns, cluster string) error

	// ClusterNodesCounts return the count of cluster
	ClusterNodesCounts(ns, cluster string) (int, error)
}

// Abstraction of physical storage, memory and etcd implement interface
// BaseStorage wraps the Namespace and Cluster methods of a backing data store.
type BaseStorage interface {
	NamespaceStorage
	ClusterStorage
}

// ShardStorage wraps the Shard methods of a backing data store.
type ShardStorage interface {
	// ListShard return the list of name of Shard under the specified cluster
	ListShard(ns, cluster string) ([]metadata.Shard, error)

	// GetShard retun the shard under the specified cluster
	GetShard(ns, cluster string, shardIdx int) (*metadata.Shard, error)

	// CreateShard add a shard under the specified cluster
	CreateShard(ns, cluster string, shard *metadata.Shard) error

	// RemoveShard delete the shard under the specified cluster
	RemoveShard(ns, cluster string, shardIdx int) error

	// HasSlot return an indicator whether the slot under the specified Shard
	HasSlot(ns, cluster string, shardIdx, slot int) (bool, error)

	// AddShardSlots add slotRanges to the specified shard under the specified cluster
	AddShardSlots(ns, cluster string, shardIdx int, slotRanges []metadata.SlotRange) error

	// AddShardSlots delete slotRanges from the specified shard under the specified cluster
	RemoveShardSlots(ns, cluster string, shardIdx int, slotRanges []metadata.SlotRange) error

	// MigrateSlot delete slot from sourceIdx, and add slot to targetIdx
	MigrateSlot(ns, cluster string, sourceIdx, targetIdx, slot int) error
}

// NodeStorage wraps the Node methods of a backing data store.
type NodeStorage interface {
	// ListNodes return the list of nodes under the specified shard
	ListNodes(ns, cluster string, shardIdx int) ([]metadata.NodeInfo, error)

	// GetMasterNode return the master of node under the specified shard
	GetMasterNode(ns, cluster string, shardIdx int) (metadata.NodeInfo, error)

	// CreateNode add a node under the specified shard
	CreateNode(ns, cluster string, shardIdx int, node *metadata.NodeInfo) error

	// RemoveNode delete the node from the specified shard
	RemoveNode(ns, cluster string, shardIdx int, nodeID string) error

	// RemoveMasterNode delete the master node from the specified shard
	RemoveMasterNode(ns, cluster string, shardIdx int, nodeID string) error

	// UpdateNode update the exist node under the specified shard
	UpdateNode(ns, cluster string, shardIdx int, node metadata.NodeInfo) error
}

// Election wraps the methods of leader election and switch.
type Election interface {
	// Self return storage id
	Self() string

	// Leader return leader id
	Leader() string

	// SelfLeader return whether myself is the leader
	SelfLeader() bool

	// BecomeLeader return chan for publish leader change
	BecomeLeader() <-chan uint64

	// LeaderCampaign propose leader election
	LeaderCampaign()

	// LeaderObserve observe leader change
	LeaderObserve()

	// Stop release leadership
	Stop() error
}

// Publish wraps the methods of notify storage change event.
type Publish interface {
	// Notify return chan for publish topo update event
	Notify() <-chan Event

	// EmitEvent send topo update event to notify chan
	EmitEvent(event Event)
}

// Abstraction of logical storage, include all methods of TopoMeta
type TopoStorage interface {
	BaseStorage
	ShardStorage
	NodeStorage
	Election
	Publish
}

// Abstraction of migrate storage, export to migrate submodel
type MigrateStorage interface {
	// PushMigrateTask push migrate task to queue back
	AddMigrateTask(ns, cluster string, tasks []*etcd.MigrateTask) error

	// PopMigrateTask pop migrate task from queue front
	RemoveMigrateTask(task *etcd.MigrateTask) error

	// GetMigrateTasks return migrate tasks
	GetMigrateTasks(ns, cluster string) ([]*etcd.MigrateTask, error)

	// UpdateMigrateTaskDoing update doing maigrate task info
	AddDoingMigrateTask(task *etcd.MigrateTask) error

	// GetMigrateTaskDoing return doing maigrate task info
	GetMigrateTaskDoing(ns, cluster string) (*etcd.MigrateTask, error)

	// AddMigrateTaskHistory add maigrate task to history record
	AddMigrateTaskHistory(task *etcd.MigrateTask) error

	// GetMigrateTaskHistory return the list of maigrate tasks of history records
	GetMigrateTaskHistory(ns, cluster string) ([]*etcd.MigrateTask, error)

	// HasMigrateTaskHistory return an indicator whether the cluster have the maigrate task is history
	HasMigrateTaskHistory(task *etcd.MigrateTask) (bool, error)

	// HasMigrateTask return an indicator whether the cluster have the maigrate task
	HasMigrateTask(ns, cluster string, taskID uint64) (bool, error)
}

// Abstraction of failover storage, export to failover submodel
type FailoverStorage interface {
	// UpdateFailoverTaskDoing update doing failover task info
	UpdateFailoverTaskDoing(task *etcd.FailoverTask) error

	// GetFailoverTaskDoing return doing failover task info
	GetFailoverTaskDoing(ns, cluster string) (*etcd.FailoverTask, error)

	// AddFailoverHistory add failover task to history record
	AddFailoverHistory(task *etcd.FailoverTask) error

	// GetFailoverHistory return the list of failover tasks of history records
	GetFailoverHistory(ns, cluster string) ([]*etcd.FailoverTask, error)
}

// MetaStorage contains all the methods required by the high level
type MetaStorage interface {
	TopoStorage
	MigrateStorage
	FailoverStorage
	io.Closer
}
