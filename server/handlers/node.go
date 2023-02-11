package handlers

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

func ListNode(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}

	storage := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	nodes, err := storage.ListNodes(c, ns, cluster, shard)
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, nodes)
}

func CreateNode(c *gin.Context) {
	var nodeInfo metadata.NodeInfo
	if err := c.BindJSON(&nodeInfo); err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}
	if err := nodeInfo.Validate(); err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}

	storage := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	err = storage.CreateNode(c, ns, cluster, shard, &nodeInfo)
	switch err {
	case nil:
		responseOK(c, "Created")
	case metadata.ErrClusterHasExisted:
		responseErrorWithCode(c, http.StatusConflict, "")
	default:
		responseError(c, err)
	}
}

func RemoveNode(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	id := c.Param("id")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}

	storage := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	if err := storage.RemoveNode(c, ns, cluster, shard, id); err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, "OK")
}

func FailoverNode(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	id := c.Param("id")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}

	storage := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	nodes, err := storage.ListNodes(c, ns, cluster, shard)
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
