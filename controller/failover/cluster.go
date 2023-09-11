package failover

import (
	"sync"
	"time"

	"golang.org/x/net/context"

	"go.uber.org/zap"

	"github.com/RocksLabs/kvrocks_controller/config"
	"github.com/RocksLabs/kvrocks_controller/logger"
	"github.com/RocksLabs/kvrocks_controller/metadata"
	"github.com/RocksLabs/kvrocks_controller/storage"
	"github.com/RocksLabs/kvrocks_controller/util"
)

type ClusterConfig struct {
	PingInterval    int
	MaxPingCount    int
	MinAliveSize    int
	MaxFailureRatio float64
}

type Cluster struct {
	namespace string
	cluster   string
	config    *ClusterConfig
	storage   *storage.Storage
	tasks     map[string]*storage.FailoverTask
	tasksIdx  []string

	quitCh    chan struct{}
	closeOnce sync.Once
	rw        sync.RWMutex
}

// NewCluster return a Cluster instance and start schedule goroutine
func NewCluster(ns, cluster string, stor *storage.Storage, failOverCfg *config.FailOverConfig) *Cluster {
	fn := &Cluster{
		namespace: ns,
		cluster:   cluster,
		storage:   stor,
		tasks:     make(map[string]*storage.FailoverTask),
		quitCh:    make(chan struct{}),
		config:    buildClusterConfig(failOverCfg),
	}
	go fn.loop()
	return fn
}

// Close will release the resource when closing
func (c *Cluster) Close() error {
	c.closeOnce.Do(func() {
		close(c.quitCh)
	})
	return nil
}

func (c *Cluster) AddTask(task *storage.FailoverTask) error {
	c.rw.Lock()
	defer c.rw.Unlock()
	if task == nil {
		return nil
	}
	if _, ok := c.tasks[task.Node.Addr]; ok {
		return nil
	}
	task.Status = TaskQueued
	c.tasks[task.Node.Addr] = task
	c.tasksIdx = append(c.tasksIdx, task.Node.Addr)
	return nil
}

func (c *Cluster) RemoveNodeTask(addr string) {
	c.rw.Lock()
	defer c.rw.Unlock()
	if _, ok := c.tasks[addr]; !ok {
		return
	}
	targetIndex := -1
	for i, nodeAddr := range c.tasksIdx {
		if addr == nodeAddr {
			targetIndex = i
			break
		}
	}
	c.removeTask(targetIndex)
}

func (c *Cluster) GetTasks() ([]*storage.FailoverTask, error) {
	c.rw.RLock()
	defer c.rw.RUnlock()
	var tasks []*storage.FailoverTask
	for _, task := range c.tasks {
		tasks = append(tasks, task)
	}
	return tasks, nil
}

// IsEmpty return an indicator whether the tasks queue has tasks, callend gcClusters
func (c *Cluster) IsEmpty() bool {
	c.rw.Lock()
	defer c.rw.Unlock()
	return len(c.tasksIdx) == 0
}

// removeTask is not goroutine safety, assgin caller hold mutex
func (c *Cluster) removeTask(idx int) {
	if idx < 0 || idx >= len(c.tasksIdx) {
		return
	}
	node := c.tasksIdx[idx]
	c.tasksIdx = append(c.tasksIdx[:idx], c.tasksIdx[idx+1:]...)
	delete(c.tasks, node)
}

func (c *Cluster) purgeTasks() {
	c.rw.Lock()
	defer c.rw.Unlock()
	for node := range c.tasks {
		delete(c.tasks, node)
	}
	c.tasksIdx = c.tasksIdx[0:0]
	return
}

func (c *Cluster) loop() {
	ctx := context.Background()
	ticker := time.NewTicker(time.Duration(c.config.PingInterval) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.rw.RLock()
			nodesCount, err := c.storage.ClusterNodesCounts(ctx, c.namespace, c.cluster)
			if err != nil {
				c.rw.RUnlock()
				break
			}
			if nodesCount > c.config.MinAliveSize && float64(len(c.tasks))/float64(nodesCount) > c.config.MaxFailureRatio {
				logger.Get().Sugar().Warnf("safe mode, loop ratio %.2f, allnodes: %d, failnodes: %d",
					c.config.MaxFailureRatio, nodesCount, len(c.tasks),
				)
				c.purgeTasks()
				c.rw.RUnlock()
				break
			}
			for idx, nodeAddr := range c.tasksIdx {
				if _, ok := c.tasks[nodeAddr]; !ok {
					continue
				}
				task := c.tasks[nodeAddr]
				c.removeTask(idx)
				if task.Type == ManualType {
					c.promoteMaster(ctx, task)
					continue
				}
				if err := util.PingCmd(ctx, &task.Node); err == nil {
					continue
				}
				c.promoteMaster(ctx, task)
			}
			c.rw.RUnlock()
		case <-c.quitCh:
			return
		}
	}
}

func (c *Cluster) promoteMaster(ctx context.Context, task *storage.FailoverTask) {
	task.Status = TaskStarted
	task.StartTime = time.Now().Unix()
	var err error
	if task.Node.Role == metadata.RoleMaster {
		err = c.storage.PromoteNewMaster(ctx, c.namespace, c.cluster, task.ShardIdx, task.Node.ID)
	}
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
	_ = c.storage.AddFailOverHistory(ctx, task)
}

func buildClusterConfig(cfg *config.FailOverConfig) *ClusterConfig {
	return &ClusterConfig{
		PingInterval:    cfg.PingIntervalSeconds,
		MaxPingCount:    cfg.MaxPingCount,
		MinAliveSize:    cfg.MinAliveSize,
		MaxFailureRatio: cfg.MaxFailureRatio,
	}
}
