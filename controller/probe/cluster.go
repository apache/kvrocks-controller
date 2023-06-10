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
	"errors"
	"time"

	"github.com/RocksLabs/kvrocks_controller/controller/failover"
	"github.com/RocksLabs/kvrocks_controller/logger"
	"github.com/RocksLabs/kvrocks_controller/metadata"
	"github.com/RocksLabs/kvrocks_controller/storage"
	"github.com/RocksLabs/kvrocks_controller/util"
	"go.uber.org/zap"
)

var (
	ErrClusterNotInitialized = errors.New("CLUSTERDOWN The cluster is not initialized")
	ErrRestoringBackUp       = errors.New("LOADING kvrocks is restoring the db from backup")
)

var (
	probeInterval      = failover.PingInterval / 3
	defaultFailOverCnt = int64(15)
)

type Cluster struct {
	namespace     string
	cluster       string
	storage       *storage.Storage
	failOver      *failover.Failover
	failureCounts map[string]int64
	stopCh        chan struct{}
}

func NewCluster(ns, cluster string, storage *storage.Storage, failOver *failover.Failover) *Cluster {
	return &Cluster{
		namespace:     ns,
		cluster:       cluster,
		storage:       storage,
		failOver:      failOver,
		failureCounts: make(map[string]int64),
		stopCh:        make(chan struct{}),
	}
}

func (c *Cluster) start() {
	go c.loop()
}

func (c *Cluster) probe(ctx context.Context, cluster *metadata.Cluster) (*metadata.Cluster, error) {
	var latestEpoch int64
	var latestNode *metadata.NodeInfo

	currentClusterStr, _ := cluster.ToSlotString()
	for index, shard := range cluster.Shards {
		for _, node := range shard.Nodes {
			logger := logger.Get().With(
				zap.String("id", node.ID),
				zap.String("role", node.Role),
				zap.String("addr", node.Addr),
			)
			if _, ok := c.failureCounts[node.Addr]; !ok {
				c.failureCounts[node.Addr] = 0
			}
			info, err := util.ClusterInfoCmd(ctx, &node)
			if err != nil {
				if err.Error() == ErrRestoringBackUp.Error() {
					continue
				}
				if err.Error() == ErrClusterNotInitialized.Error() {
					// Maybe the node was restarted, just re-sync the cluster info
					clusterStr, _ := cluster.ToSlotString()
					err = util.SyncClusterInfo2Node(ctx, &node, clusterStr, cluster.Version)
					if err != nil {
						logger.With(zap.Error(err)).Warn("Failed to re-sync the cluster info")
					}
					continue
				}
				c.failureCounts[node.Addr] += 1
				if c.failureCounts[node.Addr]%defaultFailOverCnt == 0 {
					err = c.failOver.AddNode(c.namespace, c.cluster, index, node, failover.AutoType)
					logger.With(zap.Error(err)).Warn("Add the node into the fail over candidates")
				} else {
					logger.With(
						zap.Error(err),
						zap.Int64("failure_count", c.failureCounts[node.Addr]),
					).Warn("Failed to ping the node")
				}
				continue
			}
			if info.ClusterCurrentEpoch < cluster.Version {
				err := util.SyncClusterInfo2Node(ctx, &node, currentClusterStr, cluster.Version)
				if err != nil {
					logger.With(
						zap.Error(err),
						zap.Int64("cluster_version", cluster.Version),
						zap.Int64("node_version", info.ClusterCurrentEpoch),
					).Info("Failed to sync the cluster info")
				}
			}

			if info.ClusterMyEpoch > latestEpoch {
				latestEpoch = info.ClusterMyEpoch
				latestNode = &node
			}
			c.failureCounts[node.Addr] = 0
		}
	}

	if latestEpoch > cluster.Version {
		latestClusterStr, err := util.ClusterNodesCmd(ctx, latestNode)
		if err != nil {
			return nil, err
		}
		latestClusterInfo, err := metadata.ParseCluster(latestClusterStr)
		if err != nil {
			return nil, err
		}
		err = c.storage.UpdateCluster(ctx, c.namespace, latestClusterInfo)
		if err != nil {
			return nil, err
		}
		return latestClusterInfo, nil
	}
	return cluster, nil
}

func (c *Cluster) loop() {
	logger := logger.Get().With(
		zap.String("namespace", c.namespace),
		zap.String("cluster", c.cluster),
	)
	ctx := context.Background()
	probeTicker := time.NewTicker(time.Duration(probeInterval) * time.Second)
	defer probeTicker.Stop()
	for {
		select {
		case <-probeTicker.C:
			clusterInfo, err := c.storage.GetClusterInfo(ctx, c.namespace, c.cluster)
			if err != nil {
				logger.With(
					zap.Error(err),
				).Error("Failed to get the cluster info from the storage")
				break
			}
			if _, err := c.probe(ctx, clusterInfo); err != nil {
				logger.With(zap.Error(err)).Error("Failed to probe the cluster")
				break
			}
		case <-c.stopCh:
			return
		}
	}
}

func (c *Cluster) stop() {
	close(c.stopCh)
}
