package controller

import (
	"sync"

	"go.uber.org/zap"
	"github.com/KvrocksLabs/kvrocks-controller/logger"
	"github.com/KvrocksLabs/kvrocks-controller/storage"
	"github.com/KvrocksLabs/kvrocks-controller/migrate"
)

const (
	Follower = iota
	Leader
)

type Controller struct {
	stor    *storage.Storage
	mig     *migrate.Migrate
	mu      sync.Mutex
	syncers map[string]*Syncer

	stopCh       chan struct{}
	closeOnce    sync.Once
}

func New(stor *storage.Storage) (*Controller, error) {
	return &Controller{
		stor:         stor,
		mig:          migrate.NewMigrate(stor),
		syncers:      make(map[string]*Syncer, 0),
		stopCh:       make(chan struct{}),
	}, nil
}

func (c *Controller) Start() error {
	go c.syncLoop()
	return nil
}

func (c *Controller) syncLoop() {
	go c.leaderEventLoop()
	for {
		select {
		case becomeLeader :=<- c.stor.BecomeLeader():
			if becomeLeader {
				if err := c.stor.LoadCluster(); err != nil {
					logger.Get().With(
			    		zap.Error(err),
			    	).Error("load metadata from etcd error")
					c.stor.LeaderResign()
					continue
				}
				if err := c.mig.LoadData(); err != nil {
					logger.Get().With(
			    		zap.Error(err),
			    	).Error("load migrate metadata from etcd error")
			    	c.stor.LeaderResign()
			    	c.mig.Stop()
			    	continue
				}
			} else {
				c.mig.Stop()
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
	key := event.Namespace + "/" + event.Cluster
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
		case <-c.stopCh: 
			return
		}
	}
}

func (c *Controller) Stop() error {
	c.closeOnce.Do(func() {
		c.stor.Close()
		c.mig.Close()
		for _, syncer := range c.syncers {
			syncer.Close()
		}
		close(c.stopCh)
	})
	return nil
}
