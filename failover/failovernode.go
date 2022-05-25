package failover

import (
	"fmt"
	"time"
	"sync"

	"go.uber.org/zap"
	"github.com/KvrocksLabs/kvrocks-controller/logger"
	"github.com/KvrocksLabs/kvrocks-controller/util"
	"github.com/KvrocksLabs/kvrocks-controller/metadata"
	"github.com/KvrocksLabs/kvrocks-controller/storage"
	"github.com/KvrocksLabs/kvrocks-controller/storage/base/etcd"
)

// FailoverNode handler failover tasks under special cluster
type FailoverNode struct {
	namespace string
	cluster   string
	stor      *storage.Storage
	tasks     map[string]*etcd.FailoverTask
	tasksIdx  []string

	quitCh    chan struct{}
	closeOnce sync.Once
	rw        sync.RWMutex
}

// NewFailoverNode return a FailoverNode instance and start schedule goroutine
func NewFailoverNode(ns, cluster string, stor *storage.Storage) *FailoverNode {
	fn := &FailoverNode{
		namespace: ns,
		cluster:   cluster,
		stor:      stor,
		tasks:     make(map[string]*etcd.FailoverTask),
		quitCh:    make(chan struct{}),
	}
	go fn.failover()
	return fn
}

// Close be called when exit, cealr resource
func(fn *FailoverNode) Close() error{
	fn.closeOnce.Do(func() {
		close(fn.quitCh)
	})
	return nil
}

// AddFailoverTask push failover task to memory queue
func(fn *FailoverNode) AddFailoverTask(task *etcd.FailoverTask) error {
	fn.rw.Lock()
	defer fn.rw.Unlock()
	if task == nil {
		return nil
	}
	task.Status = TaskPending
	fn.tasks[task.Node.Address] = task
	fn.tasksIdx = append(fn.tasksIdx, task.Node.Address)
	return nil
}

// GetFailoverTasks returns failover queue tasks
func(fn *FailoverNode) GetFailoverTasks()([]*etcd.FailoverTask, error) {
	fn.rw.RLock()
	defer fn.rw.RUnlock()
	var ft []*etcd.FailoverTask
	for _, task := range fn.tasks {
		ft = append(ft, task)
	}
	return ft, nil
}

// Empty return an indicator whether the tasks queue has tasks, callend gcSpace
func(fn *FailoverNode) Empty() bool {
	fn.rw.Lock()
	defer fn.rw.Unlock()
	return len(fn.tasksIdx) == 0
}

// removeFailoverTask is not goroutine safety, assgin caller hold mutex
func(fn *FailoverNode) removeFailoverTask(idx int)  {
	if idx < 0 || idx >= len(fn.tasksIdx) {
		return 
	}
	node := fn.tasksIdx[idx]
	fn.tasksIdx = append(fn.tasksIdx[:idx], fn.tasksIdx[idx + 1:]...)
	delete(fn.tasks, node)
}

// clearFailoverTask is not goroutine safety, assgin caller hold mutex
func(fn *FailoverNode) clearFailoverTask()  {
	for node, _ := range fn.tasks {
		delete(fn.tasks, node)
	}
	fn.tasksIdx = fn.tasksIdx[0:0]
	return
}

// failover loop handle failover nodes
func(fn *FailoverNode) failover() {
	failoverTicker := time.NewTimer(time.Duration(FailoverInterval) * time.Second)
	defer failoverTicker.Stop()
	for {
		select {
		case <-failoverTicker.C:
			fn.rw.RLock()
			nodesCount, err := fn.stor.ClusterNodesCounts(fn.namespace, fn.cluster)
			if err != nil {
				break
			}
			if nodesCount > FailoverMinSize && float64(len(fn.tasks)) / float64(nodesCount) > FailoverRaito {
				logger.Get().Warn(fmt.Sprintf("safe mode, failover ratio %.2f, allnodes: %d, failnodes: %d", 
					FailoverRaito, nodesCount, len(fn.tasks)))
				fn.clearFailoverTask()
				break
			}
			for idx, nodeAddr := range fn.tasksIdx {
				if _, ok := fn.tasks[nodeAddr]; !ok {
					continue
				}
				task := fn.tasks[nodeAddr]
				if task.Type == ManualType {
					fn.failoverDoing(task, idx)
					continue
				}
				task.ProbeCount++
				err := util.PingCmd(nodeAddr)
				if err == nil {
					fn.removeFailoverTask(idx)
					break
				}
				if task.ProbeCount > FailoverCount {
					fn.failoverDoing(task, idx)
				}
			}
		case <-fn.quitCh:
			return 
		}
		fn.rw.RUnlock()
		failoverTicker.Reset(time.Duration(FailoverInterval) * time.Second)
	}
}

// failoverDoing do failover and update cluster info
func(fn *FailoverNode) failoverDoing(task *etcd.FailoverTask, idx int) {
	task.Status = TaskDoing
	task.DoingTime = time.Now().Unix()
	var err error
	if task.Node.Role == metadata.RoleSlave {
		err = fn.stor.RemoveNode(fn.namespace, fn.cluster, task.ShardIdx, task.Node.ID)
	} else {
		err = fn.stor.RemoveMasterNode(fn.namespace, fn.cluster, task.ShardIdx, task.Node.ID) 
	}
	fn.removeFailoverTask(idx)
	if err != nil {
		task.Status = TaskFail
		task.Err = err.Error()
		logger.Get().With(
			zap.Error(err),
			zap.Any("task", task),
		).Error("failovernode abort!!!")
	} else {
		task.Status = TaskSuccess
		logger.Get().With(
			zap.Error(err),
			zap.Any("task", task),
		).Error("failovernode finish!!!")
	}

	task.DoneTime = time.Now().Unix()
	fn.stor.AddFailoverHistory(task)
}
