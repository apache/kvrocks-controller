package controller

import (
	"sync"

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
	exitLeaderCh chan struct{}
}

func New(stor *storage.Storage) (*Controller, error) {
	return &Controller{
		stor:         stor,
		syncers:      make(map[string]*Syncer, 0),
		stopCh:       make(chan struct{}),
		exitLeaderCh: make(chan struct{}),
	}, nil
}

func (c *Controller) Start() error {
	go c.syncLoop()
	return nil
}

func (c *Controller) syncLoop() {
	for {
		select {
		case becomeLeader :=<- c.stor.BecomeLeader():
			if becomeLeader {
				if err := c.stor.LoadCluster(); err != nil {
					c.stor.Close()
					continue
				}
				mig , err:= migrate.NewMigrate(c.stor)
				if err != nil {
					c.mig.Close()
					c.stor.Close()
					continue
				}
				c.mig = mig
				go c.enterLeaderState()
			} else {
				c.mig.Close()
				c.stor.Close()
				c.exitLeaderCh <- struct{}{}
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

func (c *Controller) enterLeaderState() {
	for {
		select {
		case event := <-c.stor.Notify():
			if !c.stor.SelfLeader() {
				continue
			}
			c.handleEvent(&event)
		case <-c.exitLeaderCh: 
			return
		}
	}
}

func (c *Controller) Stop() error {
	for _, syncer := range c.syncers {
		syncer.Close()
	}
	close(c.stopCh)
	close(c.exitLeaderCh)
	if c.stor.SelfLeader() {
		c.mig.Close()
		c.stor.Close()
	}
	return nil
}
