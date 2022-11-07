package probe

import (
	"sync"

	"github.com/KvrocksLabs/kvrocks_controller/controller/failover"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/KvrocksLabs/kvrocks_controller/util"
)

type Probe struct {
	storage  *storage.Storage
	failOver *failover.FailOver
	probes   map[string]*Cluster
	ready    bool

	rw        sync.RWMutex
	quitCh    chan struct{}
	closeOnce sync.Once
}

// New return Probe contain all methods to manager loop
func New(storage *storage.Storage, failOver *failover.FailOver) *Probe {
	hp := &Probe{
		storage:  storage,
		failOver: failOver,
		probes:   make(map[string]*Cluster),
		quitCh:   make(chan struct{}),
	}
	return hp
}

func (hp *Probe) LoadTasks() error {
	hp.rw.Lock()
	defer hp.rw.Unlock()
	namespaces, err := hp.storage.ListNamespace()
	if err != nil {
		return err
	}

	probes := make(map[string]*Cluster)
	for _, namespace := range namespaces {
		clusters, err := hp.storage.ListCluster(namespace)
		if err != nil {
			return err
		}
		for _, cluster := range clusters {
			probes[util.BuildClusterKey(namespace, cluster)] = NewProbe(namespace, cluster, hp.storage, hp.failOver)
		}
	}
	for _, probe := range probes {
		probe.start()
	}
	hp.probes = probes
	hp.ready = true
	return nil
}

// Close implement io.Close interface
func (hp *Probe) Close() error {
	hp.rw.Lock()
	defer hp.rw.Unlock()
	hp.closeOnce.Do(func() {
		close(hp.quitCh)
	})
	return nil
}

// Stop all cluster loop when leader-follower switch
func (hp *Probe) Stop() error {
	hp.rw.Lock()
	defer hp.rw.Unlock()
	if !hp.ready {
		return nil
	}
	hp.ready = false
	for _, probe := range hp.probes {
		probe.stop()
	}
	return nil
}

// AddCluster add cluster loop and start
func (hp *Probe) AddCluster(ns, cluster string) {
	hp.rw.Lock()
	defer hp.rw.Unlock()
	if !hp.ready {
		return
	}
	if _, ok := hp.probes[util.BuildClusterKey(ns, cluster)]; ok {
		return
	}
	probe := NewProbe(ns, cluster, hp.storage, hp.failOver)
	probe.start()
	hp.probes[util.BuildClusterKey(ns, cluster)] = probe
	return
}

// RemoveCluster delete cluster loop and stop
func (hp *Probe) RemoveCluster(ns, cluster string) {
	hp.rw.Lock()
	defer hp.rw.Unlock()
	if _, ok := hp.probes[util.BuildClusterKey(ns, cluster)]; !ok {
		return
	}
	probe := hp.probes[util.BuildClusterKey(ns, cluster)]
	probe.stop()
	delete(hp.probes, util.BuildClusterKey(ns, cluster))
	return
}
