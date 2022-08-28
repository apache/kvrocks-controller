package controller

import (
	"sync"

	"github.com/KvrocksLabs/kvrocks_controller/failover"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/KvrocksLabs/kvrocks_controller/util"
)

type HealthProbe struct {
	storage  *storage.Storage
	failOver *failover.FailOver
	probes   map[string]*Probe
	ready    bool

	rw        sync.RWMutex
	quitCh    chan struct{}
	closeOnce sync.Once
}

// NewHealthProbe return HealthProbe contain all methods to manager probe
func NewHealthProbe(storage *storage.Storage, failOver *failover.FailOver) *HealthProbe {
	hp := &HealthProbe{
		storage:  storage,
		failOver: failOver,
		probes:   make(map[string]*Probe),
		quitCh:   make(chan struct{}),
	}
	return hp
}

func (hp *HealthProbe) LoadTasks() error {
	hp.rw.Lock()
	defer hp.rw.Unlock()
	namespaces, err := hp.storage.ListNamespace()
	if err != nil {
		return err
	}

	probes := make(map[string]*Probe)
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
func (hp *HealthProbe) Close() error {
	hp.rw.Lock()
	defer hp.rw.Unlock()
	hp.closeOnce.Do(func() {
		close(hp.quitCh)
	})
	return nil
}

// Stop all cluster probe when leader-follower switch
func (hp *HealthProbe) Stop() error {
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

// AddCluster add cluster probe and start
func (hp *HealthProbe) AddCluster(ns, cluster string) {
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

// RemoveCluster delete cluster probe and stop
func (hp *HealthProbe) RemoveCluster(ns, cluster string) {
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
