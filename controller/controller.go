package controller

import (
	"sync"

	"go.uber.org/zap"

	"github.com/KvrocksLabs/kvrocks_controller/consts"
	"github.com/KvrocksLabs/kvrocks_controller/logger"
	"github.com/KvrocksLabs/kvrocks_controller/metrics"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/KvrocksLabs/kvrocks_controller/util"
)

type Controller struct {
	stor       *storage.Storage
	processors *BatchProcessor
	mu         sync.Mutex
	syncers    map[string]*Syncer

	stopCh    chan struct{}
	closeOnce sync.Once
}

func New(p *BatchProcessor) (*Controller, error) {
	c := &Controller{
		processors: p,
		syncers:    make(map[string]*Syncer, 0),
		stopCh:     make(chan struct{}),
	}
	process, _ := c.processors.Lookup(consts.ContextKeyStorage)
	c.stor = process.(*storage.Storage)
	return c, nil
}

func (c *Controller) Start() error {
	go c.syncLoop()
	return nil
}

func (c *Controller) syncLoop() {
	go c.leaderEventLoop()
	for {
		select {
		case becomeLeader := <-c.stor.BecomeLeader():
			if becomeLeader {
				metrics.PrometheusMetrics.SwitchToLeader.Inc()
				if err := c.processors.Start(); err != nil {
					logger.Get().With(
						zap.Error(err),
					).Error("Failed to start processors")
					_ = c.processors.Stop()
				}
				logger.Get().Info("Start as the leader")
			} else {
				_ = c.processors.Stop()
				logger.Get().Info("Lost the leader campaign")
			}
		case <-c.stopCh:
			return
		}
	}
}

func (c *Controller) handleEvent(event *storage.Event) {
	if event.Namespace == "" || event.Cluster == "" {
		return
	}
	key := util.BuildClusterKey(event.Namespace, event.Cluster)
	c.mu.Lock()
	if _, ok := c.syncers[key]; !ok {
		c.syncers[key] = NewSyncer(c.stor)
	}
	syncer := c.syncers[key]
	c.mu.Unlock()

	syncer.Notify(event)
}

func (c *Controller) leaderEventLoop() {
	for {
		select {
		case event := <-c.stor.Notify():
			if !c.stor.IsLeader() {
				continue
			}
			c.handleEvent(&event)
			switch event.Type { // nolint
			case storage.EventCluster:
				process, _ := c.processors.Lookup(consts.ContextKeyHealthy)
				health := process.(*HealthProbe)
				switch event.Command {
				case storage.CommandCreate:
					health.AddCluster(event.Namespace, event.Cluster)
				case storage.CommandRemove:
					health.RemoveCluster(event.Namespace, event.Cluster)
				default:
				}
			default:
			}
		case <-c.stopCh:
			return
		}
	}
}

func (c *Controller) Stop() error {
	c.closeOnce.Do(func() {
		c.processors.Close()
		for _, syncer := range c.syncers {
			syncer.Close()
		}
		close(c.stopCh)
		util.CloseRedisClients()
	})
	return nil
}
