/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package probe

import (
	"context"
	"sync"

	"github.com/RocksLabs/kvrocks_controller/controller/failover"
	"github.com/RocksLabs/kvrocks_controller/storage"
	"github.com/RocksLabs/kvrocks_controller/util"
)

type Probe struct {
	storage  *storage.Storage
	failOver *failover.Failover
	probes   map[string]*Cluster
	ready    bool

	rw     sync.RWMutex
	quitCh chan struct{}
}

// New return Probe contain all methods to manager loop
func New(storage *storage.Storage, failOver *failover.Failover) *Probe {
	hp := &Probe{
		storage:  storage,
		failOver: failOver,
		probes:   make(map[string]*Cluster),
		quitCh:   make(chan struct{}),
	}
	return hp
}

func (p *Probe) Load(ctx context.Context) error {
	p.rw.Lock()
	defer p.rw.Unlock()
	namespaces, err := p.storage.ListNamespace(ctx)
	if err != nil {
		return err
	}

	probes := make(map[string]*Cluster)
	for _, namespace := range namespaces {
		clusters, err := p.storage.ListCluster(ctx, namespace)
		if err != nil {
			return err
		}
		for _, cluster := range clusters {
			probes[util.BuildClusterKey(namespace, cluster)] = NewCluster(namespace, cluster, p.storage, p.failOver)
		}
	}
	for _, probe := range probes {
		probe.start()
	}
	p.probes = probes
	p.ready = true
	return nil
}

// Shutdown all cluster loop when leader-follower switch
func (p *Probe) Shutdown() {
	p.rw.Lock()
	defer p.rw.Unlock()
	if !p.ready {
		return
	}
	p.ready = false
	for _, probe := range p.probes {
		probe.stop()
	}
}

// AddCluster add cluster loop and start
func (p *Probe) AddCluster(ns, cluster string) {
	p.rw.Lock()
	defer p.rw.Unlock()
	if !p.ready {
		return
	}
	if _, ok := p.probes[util.BuildClusterKey(ns, cluster)]; ok {
		return
	}
	probe := NewCluster(ns, cluster, p.storage, p.failOver)
	probe.start()
	p.probes[util.BuildClusterKey(ns, cluster)] = probe
	return
}

// RemoveCluster delete cluster loop and stop
func (p *Probe) RemoveCluster(ns, cluster string) {
	p.rw.Lock()
	defer p.rw.Unlock()
	if _, ok := p.probes[util.BuildClusterKey(ns, cluster)]; !ok {
		return
	}
	probe := p.probes[util.BuildClusterKey(ns, cluster)]
	probe.stop()
	delete(p.probes, util.BuildClusterKey(ns, cluster))
	return
}
