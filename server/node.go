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
	"strconv"
	"time"

	"github.com/RocksLabs/kvrocks_controller/metadata"
	"github.com/RocksLabs/kvrocks_controller/storage"
	"github.com/RocksLabs/kvrocks_controller/util"
	"github.com/gin-gonic/gin"
)

type NodeHandler struct {
	storage *storage.Storage
}

func (handler *NodeHandler) List(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		responseBadRequest(c, err)
		return
	}

	nodes, err := handler.storage.ListNodes(c, ns, cluster, shard)
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, gin.H{"nodes": nodes})
}

func (handler *NodeHandler) Create(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")

	var nodeInfo metadata.NodeInfo
	if err := c.BindJSON(&nodeInfo); err != nil {
		responseBadRequest(c, err)
		return
	}
	nodeInfo.CreatedAt = time.Now().Unix()
	if nodeInfo.ID == "" {
		nodeInfo.ID = util.GenerateNodeID()
	}
	if err := nodeInfo.Validate(); err != nil {
		responseBadRequest(c, err)
		return
	}
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		responseBadRequest(c, err)
		return
	}
	if err := util.DetectClusterNode(c, &nodeInfo); err != nil {
		responseBadRequest(c, err)
		return
	}

	err = handler.storage.CreateNode(c, ns, cluster, shard, &nodeInfo)
	switch err {
	case nil:
		responseCreated(c, "created")
	case metadata.ErrEntryExisted:
		responseBadRequest(c, err)
	default:
		responseError(c, err)
	}
}

func (handler *NodeHandler) Remove(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	id := c.Param("id")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		responseBadRequest(c, err)
		return
	}

	if err := handler.storage.RemoveNode(c, ns, cluster, shard, id); err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, "ok")
}
