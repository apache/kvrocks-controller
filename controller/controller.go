package controller

import (
	"fmt"
	"sync"

	"github.com/KvrocksLabs/kvrocks-controller/storage"
)

type Controller struct {
	storage storage.Storage
	mu      sync.Mutex
	syncers map[string]*Syncer

	stopCh chan struct{}
}

func New(storage storage.Storage) (*Controller, error) {
	return &Controller{
		storage: storage,
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

		becomeLeader, electCh := c.storage.BecomeLeader("")
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
	key := event.Namespace + "/" + event.Cluster
	c.mu.Lock()
	if _, ok := c.syncers[key]; !ok {
		c.syncers[key] = NewSyncer()
	}
	syncer := c.syncers[key]
	c.mu.Unlock()

	syncer.Notify(event)
	fmt.Printf("%v\n", event)
}

func (c *Controller) enterLeaderState() {
	for {
		select {
		case event := <-c.storage.Notify():
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
