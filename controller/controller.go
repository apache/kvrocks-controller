package controller

import (
	"sync"

	"github.com/KvrocksLabs/kvrocks_controller/consts"
	"github.com/KvrocksLabs/kvrocks_controller/logger"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"go.uber.org/zap"
)

type Controller struct {
	stor       *storage.Storage
	processers *Processes
	mu         sync.Mutex
	syncers    map[string]*Syncer

	stopCh    chan struct{}
	closeOnce sync.Once
}

func New(p *Processes) (*Controller, error) {
	c := &Controller{
		processers: p,
		syncers:    make(map[string]*Syncer, 0),
		stopCh:     make(chan struct{}),
	}
	process, _ := c.processers.Access(consts.ContextKeyStorage)
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
				if err := c.processers.Start(); err != nil {
					logger.Get().With(
						zap.Error(err),
					).Error("start leader error")
					_ = c.processers.Stop()
				}
				logger.Get().Info("start leader!")
			} else {
				c.processers.Stop()
				logger.Get().Info("exit leader")
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
	key := util.NsClusterJoin(event.Namespace, event.Cluster)
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
			if !c.stor.SelfLeader() {
				continue
			}
			c.handleEvent(&event)
			switch event.Type { // nolint
			case storage.EventCluster:
				process, _ := c.processers.Access(consts.ContextKeyHealthy)
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
		default:
			return
		}
	}
}

func (c *Controller) Stop() error {
	c.closeOnce.Do(func() {
		c.processers.Close()
		for _, syncer := range c.syncers {
			syncer.Close()
		}
		close(c.stopCh)
		util.RedisPoolClose()
	})
	return nil
}
