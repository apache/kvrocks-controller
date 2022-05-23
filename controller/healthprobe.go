package controller

import (
	"sync"

	"github.com/KvrocksLabs/kvrocks-controller/util"
	"github.com/KvrocksLabs/kvrocks-controller/storage"
)

// HealthProbe manager all clusters probe
type HealthProbe struct {
	stor   *storage.Storage
	probes map[string]*Probe
    ready  bool

	rw        sync.RWMutex
	quitCh    chan struct{}
	closeOnce sync.Once
}

// NewHealthProbe return HealthProbe contain all methods to manager probe
func NewHealthProbe(stor *storage.Storage)*HealthProbe {
	hp := &HealthProbe{
		stor:   stor,
		probes: make(map[string]*Probe),
		quitCh: make(chan struct{}),
	}
	return hp
}

// LoadData start exist clusters probe goroutine
func(hp *HealthProbe) LoadData() error {
	hp.rw.Lock()
	defer hp.rw.Unlock()
	namespaces, err := hp.stor.ListNamespace()
	if err != nil {
		return err
	}

	probes := make(map[string]*Probe)
	for _, namespace :=range namespaces {
		clusters, err := hp.stor.ListCluster(namespace)
		if err != nil {
			return err
		}
		for _, cluster :=range clusters {
			probes[util.NsClusterJoin(namespace, cluster)] = NewProbe(namespace, cluster, hp.stor)
		}
	}
	for _, probe :=range probes {
		probe.start()
	}
	hp.probes = probes
	hp.ready = true
	return nil
}

// Close implement io.Close interface
func(hp *HealthProbe) Close() error {
	hp.rw.Lock()
	defer hp.rw.Unlock()
	hp.closeOnce.Do(func() {
		close(hp.quitCh)
	})
	return nil
}

// Stop all cluster probe when leader-follower switch
func(hp *HealthProbe) Stop() {
	hp.rw.Lock()
	defer hp.rw.Unlock()
	if !hp.ready {
		return 
	}
	hp.ready = false
	for _, probe :=range hp.probes {
		probe.stop()
	}
	return
}

// AddCluster add cluster probe and start 
func(hp *HealthProbe) AddCluster(ns, cluster string) {
	hp.rw.Lock()
	defer hp.rw.Unlock()
	if !hp.ready {
		return 
	}
	if _, ok := hp.probes[util.NsClusterJoin(ns, cluster)]; ok {
		return 
	}
	probe := NewProbe(ns, cluster, hp.stor)
	probe.start()
	hp.probes[util.NsClusterJoin(ns, cluster)] = probe
	return
}

// RemoveCluster delete cluster probe and stop 
func(hp *HealthProbe) RemoveCluster(ns, cluster string) {
	hp.rw.Lock()
	defer hp.rw.Unlock()
	if !hp.ready {
		return
	}
	if probe, ok := hp.probes[util.NsClusterJoin(ns, cluster)]; !ok {
		return 
	} else {
		probe.stop()
	}
	delete(hp.probes, util.NsClusterJoin(ns, cluster))
	return 
}