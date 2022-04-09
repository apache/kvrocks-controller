package controller

import (
	"fmt"
	"sync"

	"github.com/KvrocksLabs/kvrocks-controller/storage"
)

// Syncer would sync the cluster topo information
// to cluster nodes when it's changed.
type Syncer struct {
	wg       sync.WaitGroup
	shutdown chan struct{}
	notifyCh chan storage.Event
}

func NewSyncer() *Syncer {
	syncer := &Syncer{
		shutdown: make(chan struct{}, 0),
		notifyCh: make(chan storage.Event, 8),
	}
	go syncer.loop()
	return syncer
}

func (syncer *Syncer) Notify(event *storage.Event) {
	syncer.notifyCh <- *event
}

func (syncer *Syncer) fetchAndSync(event *storage.Event) {
	fmt.Println(event)
}

func (syncer *Syncer) loop() {
	defer syncer.wg.Done()
	syncer.wg.Add(1)
	for {
		select {
		case event := <-syncer.notifyCh:
			syncer.fetchAndSync(&event)
		case <-syncer.shutdown:
			return
		}
	}
}

func (syncer *Syncer) Close() {
	close(syncer.shutdown)
	close(syncer.notifyCh)
	syncer.wg.Wait()
}
