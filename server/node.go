package server

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/KvrocksLabs/kvrocks_controller/consts"
	"github.com/KvrocksLabs/kvrocks_controller/controller/failover"
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
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
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
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
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}
	if err := nodeInfo.Validate(); err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}

	err = handler.storage.CreateNode(c, ns, cluster, shard, &nodeInfo)
	switch err {
	case nil:
		responseCreated(c, "Created")
	case metadata.ErrNodeHasExisted:
		responseErrorWithCode(c, http.StatusConflict, "")
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
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}

	if err := handler.storage.RemoveNode(c, ns, cluster, shard, id); err != nil {
		responseError(c, err)
		return
	}
	response(c, http.StatusNoContent, nil)
}

func (handler *NodeHandler) Failover(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	id := c.Param("id")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}

	nodes, err := handler.storage.ListNodes(c, ns, cluster, shard)
	if err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}
	var failoverNode *metadata.NodeInfo
	for i, node := range nodes {
		if strings.HasPrefix(node.ID, id) {
			failoverNode = &nodes[i]
			break
		}
	}
	if failoverNode == nil {
		responseErrorWithCode(c, http.StatusBadRequest, metadata.ErrNodeNoExists.Error())
		return
	}

	failOver, _ := c.MustGet(consts.ContextKeyFailover).(*failover.FailOver)
	err = failOver.AddNode(ns, cluster, shard, *failoverNode, failover.ManualType)
	if err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}
	responseOK(c, "OK")
}
