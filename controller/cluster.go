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
	"errors"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/store"
)

var (
	ErrClusterNotInitialized = errors.New("CLUSTERDOWN The cluster is not initialized")
	ErrRestoringBackUp       = errors.New("LOADING kvrocks is restoring the db from backup")
)

type ClusterCheckOptions struct {
	pingInterval        time.Duration
	maxFailureCount     int64
	enableSlaveHAUpdate bool
}

type ClusterChecker struct {
	options      ClusterCheckOptions
	clusterStore store.Store
	clusterMu    sync.Mutex
	cluster      *store.Cluster

	namespace   string
	clusterName string

	failureMu     sync.Mutex
	failureCounts map[string]int64
	syncCh        chan struct{}

	migrationFailureMu     sync.Mutex
	migrationFailureCounts map[string]int

	ctx      context.Context
	cancelFn context.CancelFunc

	wg sync.WaitGroup
}

func NewClusterChecker(s store.Store, ns, cluster string) *ClusterChecker {
	ctx, cancel := context.WithCancel(context.Background())
	c := &ClusterChecker{
		namespace:   ns,
		clusterName: cluster,

		clusterStore: s,
		options: ClusterCheckOptions{
			pingInterval:    time.Second * 3,
			maxFailureCount: 5,
		},
		failureCounts:          make(map[string]int64),
		syncCh:                 make(chan struct{}, 1),
		migrationFailureCounts: make(map[string]int),

		ctx:      ctx,
		cancelFn: cancel,
	}
	return c
}

func (c *ClusterChecker) Start() {
	c.wg.Add(1)
	go c.probeLoop()
	c.wg.Add(1)
	go c.migrationLoop()
}

func (c *ClusterChecker) WithPingInterval(interval time.Duration) *ClusterChecker {
	c.options.pingInterval = interval
	if c.options.pingInterval < 200*time.Millisecond {
		c.options.pingInterval = 200 * time.Millisecond
	}
	return c
}

func (c *ClusterChecker) WithMaxFailureCount(count int64) *ClusterChecker {
	c.options.maxFailureCount = count
	if c.options.maxFailureCount < 1 {
		c.options.maxFailureCount = 5
	}
	return c
}

func (c *ClusterChecker) WithSlaveHAUpdate(enable bool) *ClusterChecker {
	c.options.enableSlaveHAUpdate = enable
	return c
}

func (c *ClusterChecker) probeNode(ctx context.Context, node store.Node) (int64, error) {
	if _, ok := node.(*store.ClusterMockNode); ok {
		info, err := node.GetClusterInfo(ctx)
		if err != nil {
			return -1, err
		}
		return info.CurrentEpoch, nil
	}
	clusterInfo, err := node.GetClusterInfo(ctx)
	if err != nil {
		// We need to use the string contains to check the error message
		// since Kvrocks wrongly returns the error message with `ERR` prefix.
		// And it's fixed in PR: https://github.com/apache/kvrocks/pull/2362,
		// but we need to be compatible with the old version here.
		if strings.Contains(err.Error(), ErrRestoringBackUp.Error()) {
			return -1, ErrRestoringBackUp
		} else if strings.Contains(err.Error(), ErrClusterNotInitialized.Error()) {
			return -1, ErrClusterNotInitialized
		} else {
			return -1, err
		}
	}
	return clusterInfo.CurrentEpoch, nil
}

func (c *ClusterChecker) increaseFailureCount(shardIndex int, node store.Node) int64 {
	id := node.ID()
	c.failureMu.Lock()
	if _, ok := c.failureCounts[id]; !ok {
		c.failureCounts[id] = 0
	}
	c.failureCounts[id] += 1
	count := c.failureCounts[id]
	c.failureMu.Unlock()

	if !node.IsMaster() {
		if c.options.enableSlaveHAUpdate && count >= c.options.maxFailureCount && !node.Failed() {
			log := logger.Get().With(
				zap.String("cluster_name", c.clusterName),
				zap.String("id", node.ID()),
				zap.String("addr", node.Addr()),
				zap.Int64("failure_count", count))
			cluster, err := c.clusterStore.GetCluster(c.ctx, c.namespace, c.clusterName)
			if err != nil {
				log.Error("Failed to get the cluster info", zap.Error(err))
				return count
			}
			if err := cluster.SetNodeStatusByID(node.ID(), store.NodeStatusFailed); err != nil {
				log.Error("Failed to set slave node as failed", zap.Error(err))
				return count
			}
			if err := c.clusterStore.UpdateCluster(c.ctx, c.namespace, cluster); err != nil {
				log.Error("Failed to update the cluster", zap.Error(err))
				return count
			}
			log.Info("Marked slave node as failed due to probe failures")
		}
		return count
	}

	if count%c.options.maxFailureCount == 0 || count > c.options.maxFailureCount {
		log := logger.Get().With(
			zap.String("cluster_name", c.clusterName),
			zap.String("id", node.ID()),
			zap.Bool("is_master", node.IsMaster()),
			zap.String("addr", node.Addr()))
		cluster, err := c.clusterStore.GetCluster(c.ctx, c.namespace, c.clusterName)
		if err != nil {
			log.Error("Failed to get the cluster info", zap.Error(err))
			return count
		}
		newMasterID, err := cluster.PromoteNewMaster(c.ctx, shardIndex, node.ID(), "")
		if err != nil {
			log.Error("Failed to promote the new master", zap.Error(err))
			return count
		}
		err = c.clusterStore.UpdateCluster(c.ctx, c.namespace, cluster)
		if err != nil {
			log.Error("Failed to update the cluster", zap.Error(err))
			return count
		}
		// the node is normal if it can be elected as the new master,
		// because it requires the node is healthy.
		c.resetFailureCount(newMasterID)
		log.With(zap.String("new_master_id", newMasterID)).Info("Promote the new master")
	}
	return count
}

func (c *ClusterChecker) resetFailureCount(nodeID string) {
	c.failureMu.Lock()
	delete(c.failureCounts, nodeID)
	c.failureMu.Unlock()
}

func (c *ClusterChecker) sendSyncEvent() {
	select {
	case c.syncCh <- struct{}{}:
	case <-c.ctx.Done():
		return
	}
}

func (c *ClusterChecker) syncClusterToNodes(ctx context.Context) error {
	clusterInfo, err := c.clusterStore.GetCluster(ctx, c.namespace, c.clusterName)
	if err != nil {
		return err
	}
	version := clusterInfo.Version.Load()
	for _, shard := range clusterInfo.Shards {
		for _, node := range shard.Nodes {
			if node.Failed() {
				continue
			}
			go func(n store.Node) {
				log := logger.Get().With(
					zap.String("namespace", c.namespace),
					zap.String("cluster", c.clusterName),
					zap.Int64("version", version),
					zap.String("node_id", n.ID()),
					zap.String("addr", n.Addr()))
				// sync the clusterName to the latest version
				if err := n.SyncClusterInfo(ctx, clusterInfo); err != nil {
					log.Error("Failed to sync the cluster topology to the node", zap.Error(err))
				} else {
					log.Info("Succeed to sync the cluster topology to the node")
				}
			}(node)
		}
	}
	return nil
}

func (c *ClusterChecker) parallelProbeNodes(ctx context.Context, cluster *store.Cluster) {
	var mu sync.Mutex
	var latestNodeVersion int64 = 0
	var latestClusterNodesStr string
	var wg sync.WaitGroup

	for i, shard := range cluster.Shards {
		for _, node := range shard.Nodes {
			wg.Add(1)
			go func(shardIdx int, n store.Node) {
				defer wg.Done()
				log := logger.Get().With(
					zap.String("cluster_name", c.clusterName),
					zap.String("id", n.ID()),
					zap.Bool("is_master", n.IsMaster()),
					zap.String("addr", n.Addr()),
				)
				version, err := c.probeNode(ctx, n)
				// Don't sync the cluster info to the node if it is restoring the db from backup
				if errors.Is(err, ErrRestoringBackUp) {
					log.Error("The node is restoring the db from backup")
					return
				}
				if err != nil && !errors.Is(err, ErrClusterNotInitialized) {
					failureCount := c.increaseFailureCount(shardIdx, n)
					log.With(zap.Error(err),
						zap.Int64("failure_count", failureCount),
					).Warn("Failed to probe the node")
					return
				}
				log.Debug("Probe the clusterName node")

				clusterVersion := cluster.Version.Load()
				if version < clusterVersion {
					// sync the clusterName to the latest version
					if err := n.SyncClusterInfo(ctx, cluster); err != nil {
						log.With(zap.Error(err)).Error("Failed to sync the clusterName info")
					}
				} else if version > clusterVersion {
					log.With(
						zap.Int64("node.version", version),
						zap.Int64("clusterName.version", clusterVersion),
					).Warn("The node is in a higher version")
					mu.Lock()
					if version > latestNodeVersion {
						latestNodeVersion = version
						clusterNodesStr, errX := n.GetClusterNodesString(ctx)
						if errX != nil {
							log.With(zap.String("node", n.ID()), zap.Error(errX)).Error("Failed to get the cluster nodes info from node")
							// set empty explicitly
							latestClusterNodesStr = ""
						} else {
							latestClusterNodesStr = clusterNodesStr
						}
					}
					mu.Unlock()
				}
				c.resetFailureCount(n.ID())
			}(i, node)
		}
	}

	wg.Wait()
	if latestNodeVersion > cluster.Version.Load() && latestClusterNodesStr != "" {
		latestClusterInfo, err := store.ParseCluster(latestClusterNodesStr)
		if err != nil {
			logger.Get().With(zap.String("cluster", latestClusterNodesStr), zap.Error(err)).Error("Failed to parse the cluster info")
			return
		}
		latestClusterInfo.Name = cluster.Name
		latestClusterInfo.SetPassword(cluster.Shards[0].Nodes[0].Password())
		err = c.clusterStore.SetCluster(ctx, c.namespace, latestClusterInfo)
		if err != nil {
			logger.Get().With(zap.String("cluster", latestClusterNodesStr), zap.Error(err)).Error("Failed to update the cluster info")
			return
		}
		logger.Get().With(zap.Any("latestClusterInfo", latestClusterInfo)).Info("Refresh latest cluster info to all nodes")
	}
}

func (c *ClusterChecker) probeLoop() {
	defer c.wg.Done()
	log := logger.Get().With(
		zap.String("namespace", c.namespace),
		zap.String("clusterName", c.clusterName),
	)

	probeTicker := time.NewTicker(c.options.pingInterval)
	defer probeTicker.Stop()
	for {
		select {
		case <-probeTicker.C:
			clusterInfo, err := c.clusterStore.GetCluster(c.ctx, c.namespace, c.clusterName)
			if err != nil {
				log.Error("Failed to get the clusterName info from the clusterStore", zap.Error(err))
				break
			}
			c.clusterMu.Lock()
			c.cluster = clusterInfo
			c.clusterMu.Unlock()
			c.parallelProbeNodes(c.ctx, clusterInfo)
		case <-c.syncCh:
			if err := c.syncClusterToNodes(c.ctx); err != nil {
				log.Error("Failed to sync the clusterName to the nodes", zap.Error(err))
			}
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *ClusterChecker) updateCluster(cluster *store.Cluster) {
	c.clusterMu.Lock()
	c.cluster = cluster
	c.clusterMu.Unlock()
}

func (c *ClusterChecker) tryUpdateMigrationStatus(ctx context.Context, clonedCluster *store.Cluster) {
	log := logger.Get().With(
		zap.String("namespace", c.namespace),
		zap.String("cluster", c.clusterName))

	for i, shard := range clonedCluster.Shards {
		if !shard.IsMigrating() {
			continue
		}
		shouldFinalize := false
		sourceNode := shard.GetMasterNode()
		sourceNodeClusterInfo, err := sourceNode.GetClusterInfo(ctx)
		if err != nil {
			log.With(
				zap.Int("shard_index", i),
				zap.String("source_node", sourceNode.ID()),
			).Error("Failed to get the cluster info from the source node", zap.Error(err))
			continue
		}

		// If there is no migration information on the source node or the source node migration slot is not equal to the shard,
		// we should not clear it immediately to avoid aggressive clearing due to transient node issues.
		if sourceNodeClusterInfo.MigratingSlot == nil || (sourceNodeClusterInfo.MigratingSlot != nil &&
			!sourceNodeClusterInfo.MigratingSlot.Equal(shard.MigratingSlot.SlotRange)) {

			c.migrationFailureMu.Lock()
			c.migrationFailureCounts[sourceNode.ID()]++
			count := c.migrationFailureCounts[sourceNode.ID()]
			c.migrationFailureMu.Unlock()

			if count < 3 {
				log.Warn("Mismatch migrating slot, but within grace period",
					zap.Int("shard_index", i),
					zap.String("migrating_slot", shard.MigratingSlot.String()),
					zap.Int("failure_count", count),
				)
				continue
			}

			// Before clearing, try to verify if the target node has actually imported the slot.
			targetNode := clonedCluster.Shards[shard.TargetShardIndex].GetMasterNode()
			targetInfo, err := targetNode.GetClusterInfo(ctx)
			if err == nil && targetInfo.MigratingSlot != nil && targetInfo.MigratingSlot.Equal(shard.MigratingSlot.SlotRange) && targetInfo.MigratingState == "success" {
				log.Info("Verified migration success from target node despite source node reporting nil",
					zap.String("slot", shard.MigratingSlot.String()))
				shouldFinalize = true
				goto finalize
			}

			log.Error("Mismatch migrating slot after grace period, clearing state",
				zap.Int("shard_index", i),
				zap.String("migrating_slot", shard.MigratingSlot.String()),
			)
			clonedCluster.Shards[i].ClearMigrateState()
			if err = c.clusterStore.UpdateCluster(ctx, c.namespace, clonedCluster); err != nil {
				log.Error("Failed to update the migrate state", zap.Error(err))
				return
			}
			c.updateCluster(clonedCluster)
			c.migrationFailureMu.Lock()
			delete(c.migrationFailureCounts, sourceNode.ID())
			c.migrationFailureMu.Unlock()
			_ = c.clusterStore.RemoveMigrationTask(ctx, c.namespace, c.clusterName)
			continue
		}

		// Reset migration failure count if we see a valid status
		c.migrationFailureMu.Lock()
		delete(c.migrationFailureCounts, sourceNode.ID())
		c.migrationFailureMu.Unlock()

	finalize:

		if shard.TargetShardIndex < 0 || shard.TargetShardIndex >= len(clonedCluster.Shards) {
			log.Error("Invalid target shard index", zap.Int("index", shard.TargetShardIndex))
			return
		}

		migratingSlot := shard.MigratingSlot.String()
		if sourceNodeClusterInfo.MigratingState == "success" {
			shouldFinalize = true
		}

		switch sourceNodeClusterInfo.MigratingState {
		case "none", "start", "migrating", "running":
			continue
		case "fail":
			clonedCluster.Shards[i].ClearMigrateState()
			if err = c.clusterStore.UpdateCluster(ctx, c.namespace, clonedCluster); err != nil {
				log.Error("Failed to update the cluster", zap.Error(err))
				return
			}
			c.updateCluster(clonedCluster)
			_ = c.clusterStore.RemoveMigrationTask(ctx, c.namespace, c.clusterName)
			log.Warn("Failed to migrate the slot", zap.String("slot", migratingSlot))
		case "success":
			// handled by shouldFinalize
		default:
			clonedCluster.Shards[i].ClearMigrateState()
			if err = c.clusterStore.UpdateCluster(ctx, c.namespace, clonedCluster); err != nil {
				log.Error("Failed to update the cluster", zap.Error(err))
				return
			}
			c.updateCluster(clonedCluster)
			_ = c.clusterStore.RemoveMigrationTask(ctx, c.namespace, c.clusterName)
			log.Error("Unknown migrating state", zap.String("state", sourceNodeClusterInfo.MigratingState))
		}

		if shouldFinalize {
			// Atomic Topology Update: Update slot ranges for both shards in one Store update
			clonedCluster.Shards[i].SlotRanges = store.RemoveSlotFromSlotRanges(clonedCluster.Shards[i].SlotRanges, shard.MigratingSlot.SlotRange)
			clonedCluster.Shards[shard.TargetShardIndex].SlotRanges = store.AddSlotToSlotRanges(
				clonedCluster.Shards[shard.TargetShardIndex].SlotRanges, shard.MigratingSlot.SlotRange,
			)
			slotToFinalize := shard.MigratingSlot.SlotRange
			clonedCluster.Shards[i].ClearMigrateState()

			// Retry UpdateCluster on conflict
			for retry := 0; retry < 3; retry++ {
				if err = c.clusterStore.UpdateCluster(ctx, c.namespace, clonedCluster); err != nil {
					log.Warn("Failed to update cluster for migration finalization, retrying", zap.Error(err), zap.Int("retry", retry))
					// Fetch newest version and re-apply changes
					latestCluster, getErr := c.clusterStore.GetCluster(ctx, c.namespace, c.clusterName)
					if getErr != nil {
						log.Error("Failed to fetch latest cluster for retry", zap.Error(getErr))
						return
					}
					// Re-apply topology change to latest cluster
					latestCluster.Shards[i].SlotRanges = store.RemoveSlotFromSlotRanges(latestCluster.Shards[i].SlotRanges, slotToFinalize)
					latestCluster.Shards[shard.TargetShardIndex].SlotRanges = store.AddSlotToSlotRanges(
						latestCluster.Shards[shard.TargetShardIndex].SlotRanges, slotToFinalize,
					)
					latestCluster.Shards[i].ClearMigrateState()
					clonedCluster = latestCluster
					continue
				}
				break
			}

			if err == nil {
				log.Info("Migrate the slot successfully", zap.String("slot", migratingSlot))
				_ = c.clusterStore.RemoveMigrationTask(ctx, c.namespace, c.clusterName)
			} else {
				log.Error("Failed to update the cluster after retries", zap.Error(err))
				return
			}
			c.updateCluster(clonedCluster)
		}
	}
}

func (c *ClusterChecker) migrationLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.clusterMu.Lock()
			if c.cluster == nil {
				c.clusterMu.Unlock()
				continue
			}
			clonedCluster := c.cluster.Clone()
			c.clusterMu.Unlock()
			if clonedCluster == nil {
				continue
			}
			c.tryUpdateMigrationStatus(c.ctx, clonedCluster)
		}
	}
}

func (c *ClusterChecker) Close() {
	c.cancelFn()
	c.wg.Wait()
}
