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
package controller

import (
	"context"
	"fmt"
	"sync"

	"github.com/RocksLabs/kvrocks_controller/logger"
	"go.uber.org/zap"

	"github.com/RocksLabs/kvrocks_controller/metadata"
	"github.com/RocksLabs/kvrocks_controller/storage"
	"github.com/RocksLabs/kvrocks_controller/util"
)

// Syncer would sync the cluster topology information
// to cluster nodes when it's changed.
type Syncer struct {
	storage  *storage.Storage
	wg       sync.WaitGroup
	shutdown chan struct{}
	notifyCh chan storage.Event
}

func NewSyncer(s *storage.Storage) *Syncer {
	syncer := &Syncer{
		storage:  s,
		shutdown: make(chan struct{}, 0),
		notifyCh: make(chan storage.Event, 8),
	}
	go syncer.loop()
	return syncer
}

func (syncer *Syncer) Notify(event *storage.Event) {
	syncer.notifyCh <- *event
}

func (syncer *Syncer) handleEvent(event *storage.Event) error {
	switch event.Type {
	case storage.EventCluster, storage.EventShard, storage.EventNode:
		return syncer.handleClusterEvent(event)
	default:
		return nil
	}
}

func (syncer *Syncer) handleClusterEvent(event *storage.Event) error {
	if event.Command != storage.CommandRemove {
		cluster, err := syncer.storage.GetClusterInfo(context.Background(), event.Namespace, event.Cluster)
		if err != nil {
			return fmt.Errorf("failed to get cluster: %w", err)
		}
		return syncClusterInfoToAllNodes(context.Background(), cluster)
	}
	// TODO: Remove related cluster tasks
	return nil
}

func (syncer *Syncer) loop() {
	defer syncer.wg.Done()
	syncer.wg.Add(1)
	for {
		select {
		case event := <-syncer.notifyCh:
			if err := syncer.handleEvent(&event); err != nil {
				logger.Get().With(
					zap.Error(err),
					zap.Any("event", event),
				).Error("Failed to handle event")
			}
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

func syncClusterInfoToNode(ctx context.Context, node *metadata.NodeInfo, clusterSlotsStr string, version int64) error {
	cli, err := util.GetRedisClient(ctx, node)
	if err != nil {
		return fmt.Errorf("addr: %s, dail: %w", node.Addr, err)
	}

	err = cli.Do(ctx, "CLUSTERX", "setnodeid", node.ID).Err()
	if err != nil {
		return fmt.Errorf("addr: %s, set node id: %w", node.Addr, err)
	}
	err = cli.Do(ctx, "CLUSTERX", "setnodes", clusterSlotsStr, version).Err()
	if err != nil {
		return fmt.Errorf("addr: %s, set nodes: %w", node.Addr, err)
	}
	return nil
}

func syncClusterInfoToAllNodes(ctx context.Context, cluster *metadata.Cluster) error {
	// FIXME: should keep retry in separate routine to prevent occurring error
	// and cause update failure.
	clusterSlotsStr, err := cluster.ToSlotString()
	if err != nil {
		return err
	}
	var errs []error
	for _, shard := range cluster.Shards {
		for _, node := range shard.Nodes {
			if err := syncClusterInfoToNode(ctx, &node, clusterSlotsStr, cluster.Version); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if errs != nil {
		return fmt.Errorf("%v", errs)
	}
	return nil
}
