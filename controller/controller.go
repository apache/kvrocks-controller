package controller

import (
	"fmt"
	"os"
	"sync"

	"go.uber.org/zap"

	"github.com/KvrocksLabs/kvrocks_controller/controller/migrate"

	"github.com/KvrocksLabs/kvrocks_controller/controller/failover"

	"github.com/KvrocksLabs/kvrocks_controller/controller/probe"
	"github.com/KvrocksLabs/kvrocks_controller/logger"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/KvrocksLabs/kvrocks_controller/util"
)

type Controller struct {
	storage  *storage.Storage
	probe    *probe.Probe
	failover *failover.FailOver
	migrate  *migrate.Migrate

	mu      sync.Mutex
	syncers map[string]*Syncer

	stopCh    chan struct{}
	closeOnce sync.Once
}

func New(s *storage.Storage) (*Controller, error) {
	failover := failover.New(s)
	return &Controller{
		storage:  s,
		failover: failover,
		migrate:  migrate.New(s),
		probe:    probe.New(s, failover),
		syncers:  make(map[string]*Syncer, 0),
		stopCh:   make(chan struct{}),
	}, nil
}

func (c *Controller) Start() error {
	go c.syncLoop()
	return nil
}

func (c *Controller) loadModules() error {
	if err := c.failover.Load(); err != nil {
		return fmt.Errorf("load failover module: %w", err)
	}
	if err := c.probe.Load(); err != nil {
		return fmt.Errorf("load probe module: %w", err)
	}
	if err := c.migrate.Load(); err != nil {
		return fmt.Errorf("load failover module: %w", err)
	}
	return nil
}

func (c *Controller) unloadModules() {
	c.probe.Shutdown()
	c.failover.Shutdown()
	c.migrate.Shutdown()
}

func (c *Controller) syncLoop() {
	go c.leaderEventLoop()
	for {
		select {
		case becomeLeader := <-c.storage.BecomeLeader():
			if becomeLeader {
				if err := c.loadModules(); err != nil {
					logger.Get().With(zap.Error(err)).Error("Failed to load module, will exit")
					os.Exit(1)
				}
				logger.Get().Info("Start as the leader")
			} else {
				c.unloadModules()
				logger.Get().Info("Lost the leader campaign, will unload modules")
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
		c.syncers[key] = NewSyncer(c.storage)
	}
	syncer := c.syncers[key]
	c.mu.Unlock()

	syncer.Notify(event)
}

func (c *Controller) leaderEventLoop() {
	for {
		select {
		case event := <-c.storage.Notify():
			if !c.storage.IsLeader() {
				continue
			}
			c.handleEvent(&event)
			switch event.Type { // nolint
			case storage.EventCluster:
				switch event.Command {
				case storage.CommandCreate:
					c.probe.AddCluster(event.Namespace, event.Cluster)
				case storage.CommandRemove:
					c.probe.RemoveCluster(event.Namespace, event.Cluster)
				default:
				}
			default:
			}
		case <-c.stopCh:
			return
		}
	}
}

func (c *Controller) GetFailOver() *failover.FailOver {
	return c.failover
}

func (c *Controller) GetMigrate() *migrate.Migrate {
	return c.migrate
}

func (c *Controller) Stop() error {
	c.closeOnce.Do(func() {
		for _, syncer := range c.syncers {
			syncer.Close()
		}
		close(c.stopCh)
		util.CloseRedisClients()
		c.unloadModules()
	})
	return nil
}
