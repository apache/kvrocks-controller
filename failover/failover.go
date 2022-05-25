package failover

import (
	"time"
	"sync"
	"errors"

	"github.com/KvrocksLabs/kvrocks-controller/util"
	"github.com/KvrocksLabs/kvrocks-controller/metadata"
	"github.com/KvrocksLabs/kvrocks-controller/storage"
	"github.com/KvrocksLabs/kvrocks-controller/storage/base/etcd"
)

// Failover organ namespace and cluster failover goroutine
type Failover struct {
	stor  *storage.Storage
	space map[string]*FailoverNode // namespace/cluster -> FailoverNode
	ready bool

	closeOnce sync.Once
	quitCh    chan struct{}
	rw        sync.RWMutex
}

// NewFailover return Failover instance and start gc
func NewFailover(stor *storage.Storage) *Failover{
	f := &Failover{
		stor:   stor,
		space:  make(map[string]*FailoverNode),
		quitCh: make(chan struct{}),
	}
	go f.gcSpace()
	return f
}

// LoadData implement controller.Process interface
func(f *Failover) LoadData() error {
	f.rw.Lock()
	defer f.rw.Unlock()
	f.ready = true
	return nil
}

// Close implement controller.Process interface
func(f *Failover) Close() error {
	f.rw.Lock()
	defer f.rw.Unlock()
	f.closeOnce.Do(func(){
		for _, fn := range f.space {
			fn.Close()
		}
	})
	return nil
}

// Stop implement controller.Process interface
// finish all clusters failover goroutinue
func(f *Failover) Stop() error {
	f.rw.Lock()
	defer f.rw.Unlock()
	if !f.ready {
		return nil
	}
	f.ready = false
	for _, fn := range f.space {
		fn.Close()
	}
	return nil
}

// gcSpace collection space memory
func(f *Failover) gcSpace() {
	gcTicker := time.NewTicker(time.Duration(GcFailoverInterval) * time.Hour)
	defer gcTicker.Stop()
	for {
		select {
		case <-gcTicker.C:
			f.rw.Lock()
			for name, fn := range f.space {
				if fn.Empty() {
					delete(f.space, name)
				}
			}
			f.rw.Unlock()
		case <-f.quitCh:
			return 
		}
	}
}

// AddFailoverNode push failover node to memory queue
func(f *Failover) AddFailoverNode(ns, cluster string, shardIdx int, node metadata.NodeInfo, failoverType int) error {
	task := &etcd.FailoverTask{
		Namespace:   ns,
		Cluster:     cluster,
		ShardIdx:    shardIdx,
		Node:        node,
		Type:        failoverType,
		PendingTime: time.Now().Unix(),
		Status:      TaskPending,
	}
	return f.AddFailoverNodeTask(task)
}

// AddFailoverNodeTask push failover task to memory queue
func(f *Failover) AddFailoverNodeTask(task *etcd.FailoverTask) error {
	f.rw.Lock()
	defer f.rw.Unlock()
	if !f.ready {
		return errors.New("failover not ready")
	}
	if _, ok := f.space[util.NsClusterJoin(task.Namespace, task.Cluster)]; !ok {
		f.space[util.NsClusterJoin(task.Namespace, task.Cluster)] = NewFailoverNode(task.Namespace, task.Cluster, f.stor)
	}
	fn := f.space[util.NsClusterJoin(task.Namespace, task.Cluster)]
	fn.AddFailoverTask(task)
	return nil
}

// GetFailoverTasks return failover tasks 
func(f *Failover) GetFailoverTasks(ns, cluster string, queryType string) ([]*etcd.FailoverTask, error) {
	switch queryType {
	case "pending":
		f.rw.RLock()
		defer f.rw.RUnlock()
		if _, ok := f.space[util.NsClusterJoin(ns, cluster)]; !ok {
			return nil, nil
		}
		return f.space[util.NsClusterJoin(ns, cluster)].GetFailoverTasks()
	case "history":
		return f.stor.GetFailoverHistory(ns, cluster)
	default:
		return nil, errors.New("query type mismatch")
	}
	return nil, nil
}
