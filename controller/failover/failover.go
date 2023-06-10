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

package failover

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/RocksLabs/kvrocks_controller/metadata"
	"github.com/RocksLabs/kvrocks_controller/storage"
	"github.com/RocksLabs/kvrocks_controller/util"
)

const (
	TaskQueued = iota + 1
	TaskStarted
	TaskSuccess
	TaskFailed
)

const (
	AutoType = iota + 1
	ManualType
)

var (
	// PingInterval stands ping period, at least more than double ProbeInterval
	PingInterval = 6

	MaxPingCount = 2

	// MinAliveSize is min number of cluster nodes to enter the safe mode
	MinAliveSize = 10

	// MaxFailureRatio is gate value, more than clusters failed enter the safe mode
	MaxFailureRatio = 0.4

	GCInterval = 1
)

type Failover struct {
	storage  *storage.Storage
	clusters map[string]*Cluster
	ready    bool

	quitCh chan struct{}
	rw     sync.RWMutex
}

func New(storage *storage.Storage) *Failover {
	f := &Failover{
		storage:  storage,
		clusters: make(map[string]*Cluster),
		quitCh:   make(chan struct{}),
	}
	go f.gcClusters()
	return f
}

func (f *Failover) Load() error {
	f.rw.Lock()
	defer f.rw.Unlock()
	f.ready = true
	return nil
}

func (f *Failover) Shutdown() {
	f.rw.Lock()
	defer f.rw.Unlock()
	if !f.ready {
		return
	}
	f.ready = false
	for _, cluster := range f.clusters {
		cluster.Close()
	}
}

func (f *Failover) gcClusters() {
	gcTicker := time.NewTicker(time.Duration(GCInterval) * time.Hour)
	defer gcTicker.Stop()
	for {
		select {
		case <-gcTicker.C:
			f.rw.Lock()
			for name, cluster := range f.clusters {
				if cluster.IsEmpty() {
					cluster.Close()
					delete(f.clusters, name)
				}
			}
			f.rw.Unlock()
		case <-f.quitCh:
			return
		}
	}
}

func (f *Failover) AddNode(ns, cluster string, shardIdx int, node metadata.NodeInfo, typ int) error {
	task := &storage.FailOverTask{
		Namespace:  ns,
		Cluster:    cluster,
		ShardIdx:   shardIdx,
		Node:       node,
		Type:       typ,
		Status:     TaskQueued,
		QueuedTime: time.Now().Unix(),
	}
	return f.AddNodeTask(task)
}

func (f *Failover) AddNodeTask(task *storage.FailOverTask) error {
	f.rw.Lock()
	defer f.rw.Unlock()
	if !f.ready {
		return errors.New("the fail over module is not ready")
	}
	clusterKey := util.BuildClusterKey(task.Namespace, task.Cluster)
	if _, ok := f.clusters[clusterKey]; !ok {
		f.clusters[clusterKey] = NewCluster(task.Namespace, task.Cluster, f.storage)
	}
	cluster := f.clusters[clusterKey]
	return cluster.AddTask(task)
}

func (f *Failover) GetTasks(ctx context.Context, ns, cluster string, queryType string) ([]*storage.FailOverTask, error) {
	switch queryType {
	case "pending":
		f.rw.RLock()
		defer f.rw.RUnlock()
		clusterKey := util.BuildClusterKey(ns, cluster)
		if _, ok := f.clusters[clusterKey]; !ok {
			return nil, nil
		}
		return f.clusters[clusterKey].GetTasks()
	case "history":
		return f.storage.GetFailOverHistory(ctx, ns, cluster)
	default:
		return nil, errors.New("unknown query type")
	}
}
