package failover

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/KvrocksLabs/kvrocks_controller/logger"
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/KvrocksLabs/kvrocks_controller/storage/persistence/etcd"
	"github.com/KvrocksLabs/kvrocks_controller/util"
)

type Cluster struct {
	namespace string
	cluster   string
	storage   *storage.Storage
	tasks     map[string]*etcd.FailOverTask
	tasksIdx  []string

	quitCh    chan struct{}
	closeOnce sync.Once
	rw        sync.RWMutex
}

// NewCluster return a Cluster instance and start schedule goroutine
func NewCluster(ns, cluster string, storage *storage.Storage) *Cluster {
	fn := &Cluster{
		namespace: ns,
		cluster:   cluster,
		storage:   storage,
		tasks:     make(map[string]*etcd.FailOverTask),
		quitCh:    make(chan struct{}),
	}
	go fn.loop()
	return fn
}

// Close will release the resource when closing
func (n *Cluster) Close() error {
	n.closeOnce.Do(func() {
		close(n.quitCh)
	})
	return nil
}

func (n *Cluster) AddTask(task *etcd.FailOverTask) error {
	n.rw.Lock()
	defer n.rw.Unlock()
	if task == nil {
		return nil
	}
	if _, ok := n.tasks[task.Node.Address]; ok {
		return nil
	}
	task.Status = TaskQueued
	n.tasks[task.Node.Address] = task
	n.tasksIdx = append(n.tasksIdx, task.Node.Address)
	return nil
}

func (n *Cluster) GetTasks() ([]*etcd.FailOverTask, error) {
	n.rw.RLock()
	defer n.rw.RUnlock()
	var tasks []*etcd.FailOverTask
	for _, task := range n.tasks {
		tasks = append(tasks, task)
	}
	return tasks, nil
}

// IsEmpty return an indicator whether the tasks queue has tasks, callend gcClusters
func (n *Cluster) IsEmpty() bool {
	n.rw.Lock()
	defer n.rw.Unlock()
	return len(n.tasksIdx) == 0
}

// removeTask is not goroutine safety, assgin caller hold mutex
func (n *Cluster) removeTask(idx int) {
	if idx < 0 || idx >= len(n.tasksIdx) {
		return
	}
	node := n.tasksIdx[idx]
	n.tasksIdx = append(n.tasksIdx[:idx], n.tasksIdx[idx+1:]...)
	delete(n.tasks, node)
}

func (n *Cluster) purgeTasks() {
	for node := range n.tasks {
		delete(n.tasks, node)
	}
	n.tasksIdx = n.tasksIdx[0:0]
	return
}

func (n *Cluster) loop() {
	ticker := time.NewTicker(time.Duration(PingInterval) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			n.rw.RLock()
			nodesCount, err := n.storage.ClusterNodesCounts(n.namespace, n.cluster)
			if err != nil {
				n.rw.RUnlock()
				break
			}
			if nodesCount > MinAliveSize && float64(len(n.tasks))/float64(nodesCount) > MaxFailureRatio {
				logger.Get().Warn(fmt.Sprintf("safe mode, loop ratio %.2f, allnodes: %d, failnodes: %d",
					MaxFailureRatio, nodesCount, len(n.tasks)))
				n.purgeTasks()
				n.rw.RUnlock()
				break
			}
			for idx, nodeAddr := range n.tasksIdx {
				if _, ok := n.tasks[nodeAddr]; !ok {
					continue
				}
				task := n.tasks[nodeAddr]
				if task.Type == ManualType {
					n.failover(task, idx)
					continue
				}
				task.ProbeCount++
				if err := util.PingCmd(nodeAddr); err == nil {
					task.ProbeCount = 0
					n.removeTask(idx)
					break
				}
				if task.ProbeCount >= MaxPingCount {
					n.failover(task, idx)
				}
			}
			n.rw.RUnlock()
		case <-n.quitCh:
			return
		}
	}
}

func (n *Cluster) failover(task *etcd.FailOverTask, idx int) {
	task.Status = TaskStarted
	task.StartTime = time.Now().Unix()
	var err error
	if task.Node.Role == metadata.RoleSlave {
		err = n.storage.RemoveNode(n.namespace, n.cluster, task.ShardIdx, task.Node.ID)
	} else {
		err = n.storage.PromoteNewMaster(n.namespace, n.cluster, task.ShardIdx, task.Node.ID)
	}
	n.removeTask(idx)
	if err != nil {
		task.Status = TaskFailed
		task.Err = err.Error()
		logger.Get().With(
			zap.Error(err),
			zap.Any("task", task),
		).Error("Abort the fail over task")
	} else {
		task.Status = TaskSuccess
		logger.Get().With(zap.Any("task", task)).Info("Finish the fail over task")
	}

	task.FinishTime = time.Now().Unix()
	_ = n.storage.AddFailOverHistory(task)
}
