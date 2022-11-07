package failover

import (
	"errors"
	"sync"
	"time"

	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/KvrocksLabs/kvrocks_controller/storage/persistence/etcd"
	"github.com/KvrocksLabs/kvrocks_controller/util"
)

const (
	TaskQueued = iota + 1
	TaskStarted
	TaskSuccess
	TaskFailed
)

const (
	AutoType = iota + 1
	ManualType
)

var (
	// PingInterval stands ping period, at least more than double ProbeInterval
	PingInterval = 6

	MaxPingCount = 2

	// MinAliveSize is min number of cluster nodes to enter the safe mode
	MinAliveSize = 10

	// MaxFailureRatio is gate value, more than clusters failed enter the safe mode
	MaxFailureRatio = 0.4

	GCInterval = 1
)

type FailOver struct {
	storage  *storage.Storage
	clusters map[string]*Cluster
	ready    bool

	quitCh chan struct{}
	rw     sync.RWMutex
}

func New(storage *storage.Storage) *FailOver {
	f := &FailOver{
		storage:  storage,
		clusters: make(map[string]*Cluster),
		quitCh:   make(chan struct{}),
	}
	go f.gcClusters()
	return f
}

func (f *FailOver) Load() error {
	f.rw.Lock()
	defer f.rw.Unlock()
	f.ready = true
	return nil
}

func (f *FailOver) Shutdown() {
	f.rw.Lock()
	defer f.rw.Unlock()
	if !f.ready {
		return
	}
	f.ready = false
	for _, cluster := range f.clusters {
		cluster.Close()
	}
}

func (f *FailOver) gcClusters() {
	gcTicker := time.NewTicker(time.Duration(GCInterval) * time.Hour)
	defer gcTicker.Stop()
	for {
		select {
		case <-gcTicker.C:
			f.rw.Lock()
			for name, cluster := range f.clusters {
				if cluster.IsEmpty() {
					delete(f.clusters, name)
				}
			}
			f.rw.Unlock()
		case <-f.quitCh:
			return
		}
	}
}

func (f *FailOver) AddNode(ns, cluster string, shardIdx int, node metadata.NodeInfo, typ int) error {
	task := &etcd.FailOverTask{
		Namespace:  ns,
		Cluster:    cluster,
		ShardIdx:   shardIdx,
		Node:       node,
		Type:       typ,
		Status:     TaskQueued,
		QueuedTime: time.Now().Unix(),
	}
	return f.AddNodeTask(task)
}

func (f *FailOver) AddNodeTask(task *etcd.FailOverTask) error {
	f.rw.Lock()
	defer f.rw.Unlock()
	if !f.ready {
		return errors.New("the fail over module is not ready")
	}
	clusterKey := util.BuildClusterKey(task.Namespace, task.Cluster)
	if _, ok := f.clusters[clusterKey]; !ok {
		f.clusters[clusterKey] = NewCluster(task.Namespace, task.Cluster, f.storage)
	}
	cluster := f.clusters[clusterKey]
	return cluster.AddTask(task)
}

func (f *FailOver) GetTasks(ns, cluster string, queryType string) ([]*etcd.FailOverTask, error) {
	switch queryType {
	case "pending":
		f.rw.RLock()
		defer f.rw.RUnlock()
		clusterKey := util.BuildClusterKey(ns, cluster)
		if _, ok := f.clusters[clusterKey]; !ok {
			return nil, nil
		}
		return f.clusters[clusterKey].GetTasks()
	case "history":
		return f.storage.GetFailOverHistory(ns, cluster)
	default:
		return nil, errors.New("unknown query type")
	}
}
