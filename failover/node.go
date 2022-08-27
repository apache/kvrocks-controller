package failover

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/KvrocksLabs/kvrocks_controller/logger"
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/metrics"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/KvrocksLabs/kvrocks_controller/storage/base/etcd"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"github.com/prometheus/client_golang/prometheus"
)

type Node struct {
	namespace string
	cluster   string
	storage   *storage.Storage
	tasks     map[string]*etcd.FailoverTask
	tasksIdx  []string

	quitCh    chan struct{}
	closeOnce sync.Once
	rw        sync.RWMutex
}

// NewNode return a Node instance and start schedule goroutine
func NewNode(ns, cluster string, storage *storage.Storage) *Node {
	fn := &Node{
		namespace: ns,
		cluster:   cluster,
		storage:   storage,
		tasks:     make(map[string]*etcd.FailoverTask),
		quitCh:    make(chan struct{}),
	}
	go fn.failover()
	return fn
}

// Close will release the resource when closing
func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		close(n.quitCh)
	})
	return nil
}

func (n *Node) AddTask(task *etcd.FailoverTask) error {
	n.rw.Lock()
	defer n.rw.Unlock()
	if task == nil {
		return nil
	}
	if _, ok := n.tasks[task.Node.Address]; ok {
		return nil
	}
	task.Status = TaskPending
	n.tasks[task.Node.Address] = task
	n.tasksIdx = append(n.tasksIdx, task.Node.Address)
	return nil
}

func (n *Node) GetTasks() ([]*etcd.FailoverTask, error) {
	n.rw.RLock()
	defer n.rw.RUnlock()
	var tasks []*etcd.FailoverTask
	for _, task := range n.tasks {
		tasks = append(tasks, task)
	}
	return tasks, nil
}

// IsEmpty return an indicator whether the tasks queue has tasks, callend gcNodes
func (n *Node) IsEmpty() bool {
	n.rw.Lock()
	defer n.rw.Unlock()
	return len(n.tasksIdx) == 0
}

// removeTask is not goroutine safety, assgin caller hold mutex
func (n *Node) removeTask(idx int) {
	if idx < 0 || idx >= len(n.tasksIdx) {
		return
	}
	node := n.tasksIdx[idx]
	n.tasksIdx = append(n.tasksIdx[:idx], n.tasksIdx[idx+1:]...)
	delete(n.tasks, node)
}

func (n *Node) cleanTasks() {
	for node := range n.tasks {
		delete(n.tasks, node)
	}
	n.tasksIdx = n.tasksIdx[0:0]
	return
}

func (n *Node) failover() {
	ticker := time.NewTicker(time.Duration(PingInterval) * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			n.rw.RLock()
			if len(n.tasksIdx) == 0 {
				metrics.PrometheusMetrics.FailoverFailCount.With(
					prometheus.Labels{"namespace": n.namespace,
						"cluster": n.cluster}).Set(0.0)
			}
			nodesCount, err := n.storage.ClusterNodesCounts(n.namespace, n.cluster)
			if err != nil {
				n.rw.RUnlock()
				break
			}
			if nodesCount > MinAliveSize && float64(len(n.tasks))/float64(nodesCount) > MaxFailureRatio {
				logger.Get().Warn(fmt.Sprintf("safe mode, failover ratio %.2f, allnodes: %d, failnodes: %d",
					MaxFailureRatio, nodesCount, len(n.tasks)))
				n.cleanTasks()
				n.rw.RUnlock()
				break
			}
			for idx, nodeAddr := range n.tasksIdx {
				if _, ok := n.tasks[nodeAddr]; !ok {
					continue
				}
				task := n.tasks[nodeAddr]
				if task.Type == ManualType {
					n.doingFailOver(task, idx)
					continue
				}
				task.ProbeCount++
				if err := util.PingCmd(nodeAddr); err == nil {
					n.removeTask(idx)
					break
				}
				if task.ProbeCount >= MaxPingCount {
					n.doingFailOver(task, idx)
				}
			}
			n.rw.RUnlock()
		case <-n.quitCh:
			return
		}
	}
}

func (n *Node) doingFailOver(task *etcd.FailoverTask, idx int) {
	task.Status = TaskDoing
	task.DoingTime = time.Now().Unix()
	var err error
	if task.Node.Role == metadata.RoleSlave {
		err = n.storage.RemoveSlaveNode(n.namespace, n.cluster, task.ShardIdx, task.Node.ID)
	} else {
		err = n.storage.RemoveMasterNode(n.namespace, n.cluster, task.ShardIdx, task.Node.ID)
	}
	n.removeTask(idx)
	if err != nil {
		task.Status = TaskFailed
		task.Err = err.Error()
		logger.Get().With(
			zap.Error(err),
			zap.Any("task", task),
		).Error("Abort the fail over task")
		metrics.PrometheusMetrics.FailoverFailCount.With(
			prometheus.Labels{"namespace": n.namespace,
				"cluster": n.cluster}).Inc()
	} else {
		task.Status = TaskSuccess
		logger.Get().With(
			zap.Error(err),
			zap.Any("task", task),
		).Error("Complete the fail over task")
	}

	task.DoneTime = time.Now().Unix()
	_ = n.storage.AddFailoverHistory(task)
}
