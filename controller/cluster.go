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
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/metrics"
	"github.com/apache/kvrocks-controller/store"
)

var (
	ErrClusterNotInitialized = errors.New("CLUSTERDOWN The cluster is not initialized")
	ErrRestoringBackUp       = errors.New("LOADING kvrocks is restoring the db from backup")
)

type failoverProposal struct {
	namespace    string
	clusterName  string
	shardIndex   int
	failedNodeID string
}

type ClusterCheckOptions struct {
	pingInterval        time.Duration
	maxFailureCount     int64
	enableSlaveHAUpdate bool
	failoverOpts        store.FailoverOptions
	voteThresholdRatio  float64
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

	lastProbeMu   sync.Mutex
	lastProbeTime map[string]time.Time

	failoverProposalCh chan failoverProposal

	coordinateMu       sync.Mutex
	coordinateCtx      context.Context
	coordinateCancelFn context.CancelFunc
	coordinateDoneCh   chan struct{} // closed when coordinateLoop exits; nil when not running

	voter Voter

	syncCh chan struct{}

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
			pingInterval:       time.Second * 3,
			maxFailureCount:    5,
			failoverOpts:       store.DefaultFailoverOptions(),
			voteThresholdRatio: 0.6,
		},
		failureCounts:      make(map[string]int64),
		lastProbeTime:      make(map[string]time.Time),
		failoverProposalCh: make(chan failoverProposal, 1),
		voter:              nopVoter{},
		syncCh:             make(chan struct{}, 1),

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

func (c *ClusterChecker) WithFailoverOptions(opts store.FailoverOptions) *ClusterChecker {
	c.options.failoverOpts = opts
	return c
}

func (c *ClusterChecker) WithVoter(v Voter) *ClusterChecker {
	c.voter = v
	return c
}

func (c *ClusterChecker) WithVoteThresholdRatio(ratio float64) *ClusterChecker {
	if ratio > 0 && ratio <= 1.0 {
		c.options.voteThresholdRatio = ratio
	}
	return c
}

// ShouldVote returns a VoteResponse describing whether this node's probe data
// justifies approving a failover for the given kvrocks node. The response
// includes diagnostic fields (failure count, soft threshold, last-probe age)
// so that the requesting leader can log them when a peer votes NO, making it
// possible to answer "why didn't failover happen?" in production.
func (c *ClusterChecker) ShouldVote(nodeID string) VoteResponse {
	softThreshold := int64(math.Ceil(
		float64(c.options.maxFailureCount) * c.options.voteThresholdRatio))
	freshnessWindow := c.options.pingInterval * 2

	c.failureMu.Lock()
	count := c.failureCounts[nodeID]
	c.failureMu.Unlock()

	c.lastProbeMu.Lock()
	lastProbe := c.lastProbeTime[nodeID]
	c.lastProbeMu.Unlock()

	if lastProbe.IsZero() {
		return VoteResponse{
			Vote:          false,
			Reason:        "no probe data for node",
			SoftThreshold: softThreshold,
		}
	}

	agoMs := time.Since(lastProbe).Milliseconds()
	vote := count >= softThreshold && time.Since(lastProbe) < freshnessWindow

	var reason string
	if !vote {
		if count < softThreshold {
			reason = fmt.Sprintf("failure count %d below soft threshold %d", count, softThreshold)
		} else {
			reason = fmt.Sprintf("last probe stale (%dms ago, window %dms)",
				agoMs, freshnessWindow.Milliseconds())
		}
	}

	return VoteResponse{
		Vote:           vote,
		Reason:         reason,
		FailureCount:   count,
		SoftThreshold:  softThreshold,
		LastProbeAgoMs: agoMs,
	}
}

func (c *ClusterChecker) probeNode(ctx context.Context, node store.Node) (int64, error) {
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
	metrics.Get().NodeFailureCount.With(prometheus.Labels{
		"namespace": c.namespace, "cluster": c.clusterName, "node_id": id,
	}).Set(float64(count))

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

	if count%c.options.maxFailureCount == 0 {
		logger.Get().With(
			zap.String("cluster_name", c.clusterName),
			zap.String("id", node.ID()),
			zap.Bool("is_master", node.IsMaster()),
			zap.String("addr", node.Addr()),
			zap.Int64("failure_count", count),
		).Warn("Master failure threshold reached, proposing failover")
		select {
		case c.failoverProposalCh <- failoverProposal{
			namespace:    c.namespace,
			clusterName:  c.clusterName,
			shardIndex:   shardIndex,
			failedNodeID: node.ID(),
		}:
		default:
			// previous proposal still being processed
		}
	}
	return count
}

func (c *ClusterChecker) resetFailureCount(nodeID string) {
	c.failureMu.Lock()
	delete(c.failureCounts, nodeID)
	c.failureMu.Unlock()
	metrics.Get().NodeFailureCount.With(prometheus.Labels{
		"namespace": c.namespace, "cluster": c.clusterName, "node_id": nodeID,
	}).Set(0)
}

// pruneStaleEntries removes failure-count and probe-time map entries for nodes
// that are no longer present in the current cluster topology. It is called after
// each probe round so that removing a node from a cluster eventually frees the
// memory held for it, preventing unbounded map growth.
func (c *ClusterChecker) pruneStaleEntries(activeIDs map[string]struct{}) {
	c.failureMu.Lock()
	for id := range c.failureCounts {
		if _, active := activeIDs[id]; !active {
			delete(c.failureCounts, id)
			metrics.Get().NodeFailureCount.Delete(prometheus.Labels{
				"namespace": c.namespace, "cluster": c.clusterName, "node_id": id,
			})
		}
	}
	c.failureMu.Unlock()

	c.lastProbeMu.Lock()
	for id := range c.lastProbeTime {
		if _, active := activeIDs[id]; !active {
			delete(c.lastProbeTime, id)
		}
	}
	c.lastProbeMu.Unlock()
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
	// Snapshot active node IDs before probing so we can prune stale map entries
	// for nodes removed from the topology after all goroutines finish.
	activeIDs := make(map[string]struct{})
	for _, shard := range cluster.Shards {
		for _, node := range shard.Nodes {
			activeIDs[node.ID()] = struct{}{}
		}
	}

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
				// Record probe time regardless of outcome so ShouldVote has fresh data.
				c.lastProbeMu.Lock()
				c.lastProbeTime[n.ID()] = time.Now()
				c.lastProbeMu.Unlock()

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
					isMaster := "false"
					if n.IsMaster() {
						isMaster = "true"
					}
					metrics.Get().ProbeFailures.With(prometheus.Labels{
						"namespace": c.namespace, "cluster": c.clusterName,
						"node_id": n.ID(), "is_master": isMaster,
					}).Inc()
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
	c.pruneStaleEntries(activeIDs)

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
		// you need to clear the migration information on the controller.
		if sourceNodeClusterInfo.MigratingSlot == nil || (sourceNodeClusterInfo.MigratingSlot != nil &&
			!sourceNodeClusterInfo.MigratingSlot.Equal(shard.MigratingSlot.SlotRange)) {
			log.Error("Mismatch migrating slot",
				zap.Int("shard_index", i),
				zap.String("migrating_slot", shard.MigratingSlot.String()),
			)
			clonedCluster.Shards[i].ClearMigrateState()
			if err = c.clusterStore.UpdateCluster(ctx, c.namespace, clonedCluster); err != nil {
				log.Error("Failed to update the migrate state by UpdateCluster method", zap.Error(err))
				return
			}
			c.updateCluster(clonedCluster)
			continue
		}

		if shard.TargetShardIndex < 0 || shard.TargetShardIndex >= len(clonedCluster.Shards) {
			log.Error("Invalid target shard index", zap.Int("index", shard.TargetShardIndex))
			return
		}

		migratingSlot := shard.MigratingSlot.String()
		switch sourceNodeClusterInfo.MigratingState {
		case "none", "start":
			continue
		case "fail":
			clonedCluster.Shards[i].ClearMigrateState()
			if err = c.clusterStore.UpdateCluster(ctx, c.namespace, clonedCluster); err != nil {
				log.Error("Failed to update the cluster", zap.Error(err))
				return
			}
			c.updateCluster(clonedCluster)
			log.Warn("Failed to migrate the slot", zap.String("slot", migratingSlot))
		case "success":
			clonedCluster.Shards[i].SlotRanges = store.RemoveSlotFromSlotRanges(clonedCluster.Shards[i].SlotRanges, shard.MigratingSlot.SlotRange)
			clonedCluster.Shards[shard.TargetShardIndex].SlotRanges = store.AddSlotToSlotRanges(
				clonedCluster.Shards[shard.TargetShardIndex].SlotRanges, shard.MigratingSlot.SlotRange,
			)
			clonedCluster.Shards[i].ClearMigrateState()
			if err = c.clusterStore.UpdateCluster(ctx, c.namespace, clonedCluster); err != nil {
				log.Error("Failed to update the cluster", zap.Error(err))
				return
			} else {
				log.Info("Migrate the slot successfully", zap.String("slot", migratingSlot))
			}
			c.updateCluster(clonedCluster)
		default:
			clonedCluster.Shards[i].ClearMigrateState()
			if err = c.clusterStore.UpdateCluster(ctx, c.namespace, clonedCluster); err != nil {
				log.Error("Failed to update the cluster", zap.Error(err))
				return
			}
			c.updateCluster(clonedCluster)
			log.Error("Unknown migrating state", zap.String("state", sourceNodeClusterInfo.MigratingState))
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

// StartCoordinate starts the coordinateLoop goroutine (idempotent).
// Call only when this node is the leader.
func (c *ClusterChecker) StartCoordinate() {
	c.coordinateMu.Lock()
	defer c.coordinateMu.Unlock()
	if c.coordinateCancelFn != nil {
		return
	}
	coordCtx, cancel := context.WithCancel(context.Background())
	c.coordinateCtx = coordCtx
	c.coordinateCancelFn = cancel
	doneCh := make(chan struct{})
	c.coordinateDoneCh = doneCh
	c.wg.Add(1)
	go c.coordinateLoop(doneCh)
}

// StopCoordinate stops the coordinateLoop goroutine and blocks until it has
// fully exited. This prevents a stale coordinateLoop from racing with a newly
// started one after a leader-change. Idempotent.
func (c *ClusterChecker) StopCoordinate() {
	c.coordinateMu.Lock()
	if c.coordinateCancelFn == nil {
		c.coordinateMu.Unlock()
		return
	}
	c.coordinateCancelFn()
	c.coordinateCancelFn = nil
	doneCh := c.coordinateDoneCh
	c.coordinateDoneCh = nil
	c.coordinateMu.Unlock() // release before blocking

	if doneCh != nil {
		<-doneCh // wait for coordinateLoop goroutine to fully exit
	}
}

func (c *ClusterChecker) coordinateLoop(doneCh chan struct{}) {
	defer close(doneCh) // signal exit AFTER wg.Done so Close()'s wg.Wait is clean
	defer c.wg.Done()
	for {
		select {
		case proposal := <-c.failoverProposalCh:
			c.handleProposal(c.coordinateCtx, proposal)
		case <-c.coordinateCtx.Done():
			return
		}
	}
}

func (c *ClusterChecker) handleProposal(ctx context.Context, p failoverProposal) {
	log := logger.Get().With(
		zap.String("namespace", p.namespace),
		zap.String("cluster", p.clusterName),
		zap.Int("shard_index", p.shardIndex),
		zap.String("failed_node", p.failedNodeID),
	)
	metrics.Get().FailoverProposals.With(prometheus.Labels{
		"namespace": p.namespace, "cluster": p.clusterName,
	}).Inc()

	approved, err := c.voter.RequestVotes(ctx, VoteRequest{
		Namespace:    p.namespace,
		ClusterName:  p.clusterName,
		ShardIndex:   p.shardIndex,
		FailedNodeID: p.failedNodeID,
	})
	if err != nil {
		log.Error("Vote request failed", zap.Error(err))
		metrics.Get().FailoverBlocked.With(prometheus.Labels{
			"namespace": p.namespace, "cluster": p.clusterName, "reason": "vote_error",
		}).Inc()
		return
	}
	if !approved {
		log.Info("Failover blocked by peer vote")
		return
	}

	cluster, err := c.clusterStore.GetCluster(ctx, p.namespace, p.clusterName)
	if err != nil {
		log.Error("Failed to get cluster for failover", zap.Error(err))
		return
	}
	_, newMaster, err := cluster.PromoteNewMaster(ctx, p.shardIndex, p.failedNodeID, "", c.options.failoverOpts)
	if err != nil {
		log.Error("Failed to promote new master", zap.Error(err))
		return
	}
	if err := c.clusterStore.UpdateCluster(ctx, p.namespace, cluster); err != nil {
		log.Error("Failed to persist cluster after failover", zap.Error(err))
		return
	}
	c.resetFailureCount(newMaster.ID())
	metrics.Get().FailoverCompleted.With(prometheus.Labels{
		"namespace": p.namespace, "cluster": p.clusterName,
	}).Inc()
	log.With(zap.String("new_master", newMaster.ID())).Info("Failover completed")
}

func (c *ClusterChecker) Close() {
	c.StopCoordinate()
	c.cancelFn()
	c.wg.Wait()
}
