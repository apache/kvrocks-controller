package failover

import (
	"errors"
	"sync"
	"time"

	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/KvrocksLabs/kvrocks_controller/storage/base/etcd"
	"github.com/KvrocksLabs/kvrocks_controller/util"
)

type FailOver struct {
	storage *storage.Storage
	nodes   map[string]*Node
	ready   bool

	closeOnce sync.Once
	quitCh    chan struct{}
	rw        sync.RWMutex
}

func NewFailOver(storage *storage.Storage) *FailOver {
	f := &FailOver{
		storage: storage,
		nodes:   make(map[string]*Node),
		quitCh:  make(chan struct{}),
	}
	go f.gcNodes()
	return f
}

func (f *FailOver) LoadTasks() error {
	f.rw.Lock()
	defer f.rw.Unlock()
	f.ready = true
	return nil
}

func (f *FailOver) Close() error {
	f.rw.Lock()
	defer f.rw.Unlock()
	f.closeOnce.Do(func() {
		for _, node := range f.nodes {
			node.Close()
		}
	})
	return nil
}

func (f *FailOver) Stop() error {
	f.rw.Lock()
	defer f.rw.Unlock()
	if !f.ready {
		return nil
	}
	f.ready = false
	for _, node := range f.nodes {
		node.Close()
	}
	return nil
}

func (f *FailOver) gcNodes() {
	gcTicker := time.NewTicker(time.Duration(GCInterval) * time.Hour)
	defer gcTicker.Stop()
	for {
		select {
		case <-gcTicker.C:
			f.rw.Lock()
			for name, node := range f.nodes {
				if node.IsEmpty() {
					delete(f.nodes, name)
				}
			}
			f.rw.Unlock()
		case <-f.quitCh:
			return
		}
	}
}

func (f *FailOver) AddNode(ns, cluster string, shardIdx int, node metadata.NodeInfo, typ int) error {
	task := &etcd.FailoverTask{
		Namespace:   ns,
		Cluster:     cluster,
		ShardIdx:    shardIdx,
		Node:        node,
		Type:        typ,
		PendingTime: time.Now().Unix(),
		Status:      TaskPending,
	}
	return f.AddNodeTask(task)
}

func (f *FailOver) AddNodeTask(task *etcd.FailoverTask) error {
	f.rw.Lock()
	defer f.rw.Unlock()
	if !f.ready {
		return errors.New("the fail over module is not ready")
	}
	nodeKey := util.NsClusterJoin(task.Namespace, task.Cluster)
	if _, ok := f.nodes[util.NsClusterJoin(task.Namespace, task.Cluster)]; !ok {
		f.nodes[nodeKey] = NewNode(task.Namespace, task.Cluster, f.storage)
	}
	fn := f.nodes[nodeKey]
	return fn.AddTask(task)
}

func (f *FailOver) GetTasks(ns, cluster string, queryType string) ([]*etcd.FailoverTask, error) {
	switch queryType {
	case "pending":
		f.rw.RLock()
		defer f.rw.RUnlock()
		if _, ok := f.nodes[util.NsClusterJoin(ns, cluster)]; !ok {
			return nil, nil
		}
		return f.nodes[util.NsClusterJoin(ns, cluster)].GetTasks()
	case "history":
		return f.storage.GetFailoverHistory(ns, cluster)
	default:
		return nil, errors.New("unknown query type")
	}
}
