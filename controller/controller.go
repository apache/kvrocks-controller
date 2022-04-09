package controller

import (
	"sync"

	"github.com/KvrocksLabs/kvrocks-controller/storage"
)

type Controller struct {
	stor    storage.Storage
	mu      sync.Mutex
	syncers map[string]*Syncer

	stopCh chan struct{}
}

func New(stor storage.Storage) (*Controller, error) {
	return &Controller{
		stor:    stor,
		syncers: make(map[string]*Syncer, 0),
		stopCh:  make(chan struct{}),
	}, nil
}

func (c *Controller) Start() error {
	// TODO: generate controller id
	go c.syncLoop()
	return nil
}

func (c *Controller) syncLoop() {
	for {
		select {
		case <-c.stopCh:
			return
		default:
		}

		becomeLeader, electCh := c.stor.BecomeLeader("")
		if !becomeLeader {
			select {
			case <-electCh:
				// become follower
			case <-c.stopCh:
				return
			}
		}
		// elected as leader
		c.enterLeaderState()
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
			c.handleEvent(&event)
		case <-c.stopCh:
			return
		}
	}
}

func (c *Controller) Stop() error {
	for _, syncer := range c.syncers {
		syncer.Close()
	}
	close(c.stopCh)
	return nil
}
