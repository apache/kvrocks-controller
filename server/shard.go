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

package server

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/RocksLabs/kvrocks_controller/util"

	"github.com/RocksLabs/kvrocks_controller/consts"
	"github.com/RocksLabs/kvrocks_controller/controller/failover"
	"github.com/RocksLabs/kvrocks_controller/controller/migrate"
	"github.com/RocksLabs/kvrocks_controller/metadata"
	"github.com/RocksLabs/kvrocks_controller/storage"
	"github.com/gin-gonic/gin"
)

type ShardHandler struct {
	storage *storage.Storage
}

type SlotsRequest struct {
	Slots []string `json:"slots" validate:"required"`
}

type MigrateSlotDataRequest struct {
	Tasks []*storage.MigrationTask `json:"tasks" validate:"required"`
}

type MigrateSlotOnlyRequest struct {
	Source int                  `json:"source" validate:"required"`
	Target int                  `json:"target" validate:"required"`
	Slots  []metadata.SlotRange `json:"slots" validate:"required"`
}

type CreateShardRequest struct {
	Master *metadata.NodeInfo  `json:"master"`
	Slaves []metadata.NodeInfo `json:"slaves"`
}

func (handler *ShardHandler) List(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")

	shards, err := handler.storage.ListShard(c, ns, cluster)
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, gin.H{"shards": shards})
}

func (handler *ShardHandler) Get(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		responseBadRequest(c, err)
		return
	}

	s, err := handler.storage.GetShard(c, ns, cluster, shard)
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, gin.H{"shard": s})
}

func (handler *ShardHandler) Create(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")

	var req struct {
		Nodes    []string `json:"nodes"`
		Password string   `json:"password"`
	}
	if err := c.BindJSON(&req); err != nil {
		responseBadRequest(c, err)
		return
	}
	if len(req.Nodes) == 0 {
		responseBadRequest(c, errors.New("nodes should NOT be empty"))
		return
	}
	nodes := make([]metadata.NodeInfo, len(req.Nodes))
	now := time.Now().Unix()
	for i, nodeAddr := range req.Nodes {
		nodes[i].ID = util.GenerateNodeID()
		nodes[i].Addr = nodeAddr
		if i == 0 {
			nodes[i].Role = metadata.RoleMaster
		} else {
			nodes[i].Role = metadata.RoleSlave
		}
		nodes[i].Password = req.Password
		nodes[i].CreatedAt = now
	}
	if err := handler.storage.CreateShard(c, ns, cluster, &metadata.Shard{
		Nodes:         nodes,
		ImportSlot:    -1,
		MigratingSlot: -1,
	}); err != nil {
		responseError(c, err)
		return
	}
	responseCreated(c, "ok")
}

func (handler *ShardHandler) Remove(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		responseBadRequest(c, err)
		return
	}

	err = handler.storage.RemoveShard(c, ns, cluster, shard)
	if err != nil {
		responseError(c, err)
		return
	}
	responseData(c, http.StatusNoContent, nil)
}

func (handler *ShardHandler) UpdateSlots(c *gin.Context) {
	isAdd := c.Request.Method == http.MethodPost
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		responseBadRequest(c, err)
		return
	}
	var payload SlotsRequest
	if err := c.BindJSON(&payload); err != nil {
		responseBadRequest(c, err)
		return
	}
	slotRanges := make([]metadata.SlotRange, len(payload.Slots))
	for i, slot := range payload.Slots {
		slotRange, err := metadata.ParseSlotRange(slot)
		if err != nil {
			responseBadRequest(c, err)
			return
		}
		slotRanges[i] = *slotRange
	}

	storage := handler.storage
	if isAdd {
		err = storage.AddShardSlots(c, ns, cluster, shard, slotRanges)
	} else {
		err = storage.RemoveShardSlots(c, ns, cluster, shard, slotRanges)
	}
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, "ok")
}

func (handler *ShardHandler) MigrateSlotData(c *gin.Context) {
	var req MigrateSlotDataRequest
	if err := c.BindJSON(&req); err != nil {
		responseBadRequest(c, err)
		return
	}
	migration, _ := c.MustGet(consts.ContextKeyMigrate).(*migrate.Migrator)
	if err := migration.AddTasks(c, req.Tasks); err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, "ok")
}

func (handler *ShardHandler) MigrateSlotOnly(c *gin.Context) {
	var req MigrateSlotOnlyRequest
	if err := c.BindJSON(&req); err != nil {
		responseBadRequest(c, err)
		return
	}
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	if err := handler.storage.RemoveShardSlots(c, ns, cluster, req.Source, req.Slots); err != nil {
		responseError(c, err)
		return
	}
	if err := handler.storage.AddShardSlots(c, ns, cluster, req.Target, req.Slots); err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, "ok")
}

func (handler *ShardHandler) Failover(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		responseBadRequest(c, err)
		return
	}

	nodes, err := handler.storage.ListNodes(c, ns, cluster, shard)
	if err != nil {
		return
	}
	if len(nodes) <= 1 {
		responseBadRequest(c, errors.New("no node to be failover"))
		return
	}
	var failoverNode *metadata.NodeInfo
	for i, node := range nodes {
		if node.Role == metadata.RoleMaster {
			failoverNode = &nodes[i]
			break
		}
	}
	if failoverNode == nil {
		responseBadRequest(c, metadata.ErrEntryNoExists)
		return
	}

	failOver, _ := c.MustGet(consts.ContextKeyFailover).(*failover.FailOver)
	err = failOver.AddNode(ns, cluster, shard, *failoverNode, failover.ManualType)
	if err != nil {
		responseBadRequest(c, err)
		return
	}
	responseOK(c, "ok")
}
