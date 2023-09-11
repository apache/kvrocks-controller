package failover

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/RocksLabs/kvrocks_controller/config"
	"github.com/RocksLabs/kvrocks_controller/metadata"
	"github.com/RocksLabs/kvrocks_controller/storage"
	"github.com/RocksLabs/kvrocks_controller/util"
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

type Failover struct {
	storage  *storage.Storage
	config   *config.FailOverConfig
	clusters map[string]*Cluster

	ready  bool
	quitCh chan struct{}
	rw     sync.RWMutex
}

func New(storage *storage.Storage, failOverConfig *config.FailOverConfig) *Failover {
	f := &Failover{
		storage:  storage,
		config:   failOverConfig,
		clusters: make(map[string]*Cluster),
		quitCh:   make(chan struct{}),
	}
	go f.gcClusters()
	return f
}

func (f *Failover) Load() error {
	f.rw.Lock()
	defer f.rw.Unlock()
	f.ready = true
	return nil
}

func (f *Failover) Shutdown() {
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

func (f *Failover) gcClusters() {
	gcTicker := time.NewTicker(time.Duration(f.config.GCIntervalSeconds) * time.Second)
	defer gcTicker.Stop()
	for {
		select {
		case <-gcTicker.C:
			f.rw.Lock()
			for name, cluster := range f.clusters {
				if cluster.IsEmpty() {
					cluster.Close()
					delete(f.clusters, name)
				}
			}
			f.rw.Unlock()
		case <-f.quitCh:
			return
		}
	}
}

func (f *Failover) AddNode(ns, cluster string, shardIdx int, node metadata.NodeInfo, typ int) error {
	task := &storage.FailoverTask{
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

func (f *Failover) AddNodeTask(task *storage.FailoverTask) error {
	f.rw.Lock()
	defer f.rw.Unlock()
	if !f.ready {
		return errors.New("the fail over module is not ready")
	}
	clusterKey := util.BuildClusterKey(task.Namespace, task.Cluster)
	if _, ok := f.clusters[clusterKey]; !ok {
		f.clusters[clusterKey] = NewCluster(task.Namespace, task.Cluster, f.storage, f.config)
	}
	cluster := f.clusters[clusterKey]
	return cluster.AddTask(task)
}

func (f *Failover) GetTasks(ctx context.Context, ns, cluster string, queryType string) ([]*storage.FailoverTask, error) {
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
		return f.storage.GetFailOverHistory(ctx, ns, cluster)
	default:
		return nil, errors.New("unknown query type")
	}
}

func (f *Failover) GetConfiguredPingInterval() int {
	return f.config.PingIntervalSeconds
}
