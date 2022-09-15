package handlers

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/KvrocksLabs/kvrocks_controller/consts"
	"github.com/KvrocksLabs/kvrocks_controller/failover"
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"github.com/gin-gonic/gin"
)

func ListNode(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		util.ResponseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}

	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	nodes, err := stor.ListNodes(ns, cluster, shard)
	if err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeNoExists {
			util.ResponseErrorWithCode(c, http.StatusNotFound, err.Error())
		} else {
			util.ResponseError(c, err.Error())
		}
		return
	}
	util.ResponseOK(c, nodes)
}

func CreateNode(c *gin.Context) {
	var nodeInfo metadata.NodeInfo
	if err := c.BindJSON(&nodeInfo); err != nil {
		util.ResponseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}
	if err := nodeInfo.Validate(); err != nil {
		util.ResponseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		util.ResponseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}

	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	if err := stor.CreateNode(ns, cluster, shard, &nodeInfo); err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeExisted {
			util.ResponseErrorWithCode(c, http.StatusConflict, err.Error())
		} else {
			util.ResponseError(c, err.Error())
		}
		return
	}
	util.ResponseCreated(c, "OK")
}

func RemoveNode(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	id := c.Param("id")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		util.ResponseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}

	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	if err := stor.RemoveSlaveNode(ns, cluster, shard, id); err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeNoExists {
			util.ResponseErrorWithCode(c, http.StatusBadRequest, err.Error())
		} else {
			util.ResponseError(c, err.Error())
		}
		return
	}
	util.ResponseOK(c, "OK")
}

func FailoverNode(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	id := c.Param("id")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		util.ResponseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}

	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	nodes, err := stor.ListNodes(ns, cluster, shard)
	if err != nil {
		util.ResponseErrorWithCode(c, http.StatusBadRequest, err.Error())
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
		util.ResponseErrorWithCode(c, http.StatusBadRequest, metadata.ErrNodeNoExists.Error())
		return
	}

	failOver, _ := c.MustGet(consts.ContextKeyFailover).(*failover.FailOver)
	err = failOver.AddNode(ns, cluster, shard, *failoverNode, failover.ManualType)
	if err != nil {
		util.ResponseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}
	util.ResponseOK(c, "OK")
}
