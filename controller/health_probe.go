package controller

import (
	"sync"

	"github.com/KvrocksLabs/kvrocks_controller/failover"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/KvrocksLabs/kvrocks_controller/util"
)

// HealthProbe manager all clusters probe
type HealthProbe struct {
	stor   *storage.Storage
	nfor   *failover.Failover
	probes map[string]*Probe
	ready  bool

	rw        sync.RWMutex
	quitCh    chan struct{}
	closeOnce sync.Once
}

// NewHealthProbe return HealthProbe contain all methods to manager probe
func NewHealthProbe(stor *storage.Storage, nfor *failover.Failover) *HealthProbe {
	hp := &HealthProbe{
		stor:   stor,
		nfor:   nfor,
		probes: make(map[string]*Probe),
		quitCh: make(chan struct{}),
	}
	return hp
}

// LoadData start exist clusters probe goroutine
func (hp *HealthProbe) LoadData() error {
	hp.rw.Lock()
	defer hp.rw.Unlock()
	namespaces, err := hp.stor.ListNamespace()
	if err != nil {
		return err
	}

	probes := make(map[string]*Probe)
	for _, namespace := range namespaces {
		clusters, err := hp.stor.ListCluster(namespace)
		if err != nil {
			return err
		}
		for _, cluster := range clusters {
			probes[util.NsClusterJoin(namespace, cluster)] = NewProbe(namespace, cluster, hp.stor, hp.nfor)
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
	if _, ok := hp.probes[util.NsClusterJoin(ns, cluster)]; ok {
		return
	}
	probe := NewProbe(ns, cluster, hp.stor, hp.nfor)
	probe.start()
	hp.probes[util.NsClusterJoin(ns, cluster)] = probe
	return
}

// RemoveCluster delete cluster probe and stop
func (hp *HealthProbe) RemoveCluster(ns, cluster string) {
	hp.rw.Lock()
	defer hp.rw.Unlock()
	if _, ok := hp.probes[util.NsClusterJoin(ns, cluster)]; !ok {
		return
	}
	probe := hp.probes[util.NsClusterJoin(ns, cluster)]
	probe.stop()
	delete(hp.probes, util.NsClusterJoin(ns, cluster))
	return
}
