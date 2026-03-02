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
package api

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/server/helper"
	"github.com/apache/kvrocks-controller/store"
)

type ShardHandler struct {
	s store.Store
}

type SlotsRequest struct {
	Slots []string `json:"slots" validate:"required"`
}

type CreateShardRequest struct {
	Master *store.ClusterNode  `json:"master"`
	Slaves []store.ClusterNode `json:"slaves"`
}

func (handler *ShardHandler) List(c *gin.Context) {
	cluster, _ := c.MustGet(consts.ContextKeyCluster).(*store.Cluster)
	helper.ResponseOK(c, gin.H{"shards": cluster.Shards})
}

func (handler *ShardHandler) Get(c *gin.Context) {
	shard, _ := c.MustGet(consts.ContextKeyClusterShard).(*store.Shard)
	helper.ResponseOK(c, gin.H{"shard": shard})
}

func (handler *ShardHandler) Create(c *gin.Context) {
	ns := c.Param("namespace")
	var req struct {
		Nodes    []string `json:"nodes" validate:"required"`
		Password string   `json:"password"`
	}
	if err := c.BindJSON(&req); err != nil {
		helper.ResponseBadRequest(c, err)
		return
	}
	if len(req.Nodes) == 0 {
		helper.ResponseBadRequest(c, errors.New("nodes should NOT be empty"))
		return
	}
	nodes := make([]store.Node, 0, len(req.Nodes))
	for i, addr := range req.Nodes {
		node := store.NewClusterNode(addr, req.Password)
		if i == 0 {
			node.SetRole(store.RoleMaster)
		} else {
			node.SetRole(store.RoleSlave)
		}
		nodes = append(nodes, node)
	}
	cluster, _ := c.MustGet(consts.ContextKeyCluster).(*store.Cluster)
	newShard := store.NewShard()
	newShard.Nodes = nodes
	cluster.Shards = append(cluster.Shards, newShard)
	if err := handler.s.UpdateCluster(c, ns, cluster); err != nil {
		helper.ResponseError(c, err)
		return
	}
	helper.ResponseCreated(c, gin.H{"shard": newShard})
}

func (handler *ShardHandler) Remove(c *gin.Context) {
	ns := c.Param("namespace")
	shardIdx, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		helper.ResponseBadRequest(c, err)
		return
	}
	cluster, _ := c.MustGet(consts.ContextKeyCluster).(*store.Cluster)

	if shardIdx < 0 || shardIdx >= len(cluster.Shards) {
		helper.ResponseBadRequest(c, consts.ErrIndexOutOfRange)
		return
	}
	if cluster.Shards[shardIdx].IsServicing() {
		helper.ResponseBadRequest(c, consts.ErrShardIsServicing)
		return
	}
	cluster.Shards = append(cluster.Shards[:shardIdx], cluster.Shards[shardIdx+1:]...)
	if err := handler.s.UpdateCluster(c, ns, cluster); err != nil {
		helper.ResponseError(c, err)
		return
	}
	helper.ResponseNoContent(c)
}

// FailoverOpts holds optional parameters for manual failover.
type FailoverOpts struct {
	ForceOnTimeout bool `json:"force_on_timeout"`
	SyncTimeoutMs  int  `json:"sync_timeout_ms"`  // 0 means use default
	PauseTimeoutMs int  `json:"pause_timeout_ms"` // 0 means use default
}

func (handler *ShardHandler) Failover(c *gin.Context) {
	ns := c.Param("namespace")
	cluster, _ := c.MustGet(consts.ContextKeyCluster).(*store.Cluster)

	var req struct {
		PreferredNodeID string         `json:"preferred_node_id"`
		Options         *FailoverOpts `json:"options"`
	}
	if c.Request.Body != nil {
		if err := c.ShouldBindJSON(&req); err != nil {
			helper.ResponseBadRequest(c, err)
			return
		}
	}
	if len(req.PreferredNodeID) > 0 && len(req.PreferredNodeID) != store.NodeIDLen {
		helper.ResponseBadRequest(c, fmt.Errorf("invalid node id: %s", req.PreferredNodeID))
		return
	}

	opts := store.DefaultFailoverOptions()
	if req.Options != nil {
		if req.Options.SyncTimeoutMs > 0 {
			opts.SyncTimeout = time.Duration(req.Options.SyncTimeoutMs) * time.Millisecond
		}
		if req.Options.PauseTimeoutMs > 0 {
			opts.PauseDuration = time.Duration(req.Options.PauseTimeoutMs) * time.Millisecond
		}
		opts.ForceOnTimeout = req.Options.ForceOnTimeout
	}

	shardIndex, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		helper.ResponseBadRequest(c, err)
		return
	}
	oldMaster, newMaster, err := cluster.PromoteNewMaster(c, shardIndex, "", req.PreferredNodeID, opts)
	if err != nil {
		helper.ResponseError(c, err)
		return
	}

	unpauseOldMaster := func() {
		if !opts.WaitForSync {
			return
		}
		if e := oldMaster.UnpauseClient(c); e != nil {
			logger.Get().With(zap.Error(e), zap.String("node", oldMaster.Addr())).Error("Failed to unpause old master")
		}
	}

	if err := handler.s.UpdateCluster(c, ns, cluster); err != nil {
		unpauseOldMaster()
		helper.ResponseError(c, err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if e := oldMaster.SyncClusterInfo(c, cluster); e != nil {
			logger.Get().With(zap.Error(e), zap.String("node", oldMaster.Addr())).Warn("Failed to sync cluster info to old master")
		}
	}()
	go func() {
		defer wg.Done()
		if e := newMaster.SyncClusterInfo(c, cluster); e != nil {
			logger.Get().With(zap.Error(e), zap.String("node", newMaster.Addr())).Warn("Failed to sync cluster info to new master")
		}
	}()
	wg.Wait()

	unpauseOldMaster()
	helper.ResponseOK(c, gin.H{"new_master_id": newMaster.ID()})
}
