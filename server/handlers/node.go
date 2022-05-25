package handlers

import (
	"strings"
	"net/http"
	"strconv"

	"github.com/KvrocksLabs/kvrocks-controller/consts"
	"github.com/KvrocksLabs/kvrocks-controller/metadata"
	"github.com/KvrocksLabs/kvrocks-controller/storage"
	"github.com/KvrocksLabs/kvrocks-controller/failover"
	"github.com/KvrocksLabs/kvrocks-controller/util"
	"github.com/gin-gonic/gin"
)

func ListNode(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		c.JSON(http.StatusBadRequest, util.MakeFailureResponse(err.Error()))
		return
	}

	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	nodes, err := stor.ListNodes(ns, cluster, shard)
	if err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeNoExists {
			c.JSON(http.StatusNotFound, util.MakeFailureResponse(err.Error()))
		} else {
			c.JSON(http.StatusInternalServerError, util.MakeFailureResponse(err.Error()))
		}
		return
	}
	c.JSON(http.StatusOK, util.MakeSuccessResponse(nodes))
}

func CreateNode(c *gin.Context) {
	var nodeInfo metadata.NodeInfo
	if err := c.BindJSON(&nodeInfo); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"err": err.Error()})
		return
	}
	if err := nodeInfo.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"err": err.Error()})
		return
	}
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		c.JSON(http.StatusBadRequest, util.MakeFailureResponse(err.Error()))
		return
	}

	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	if err := stor.CreateNode(ns, cluster, shard, &nodeInfo); err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeExisted {
			c.JSON(http.StatusConflict, util.MakeFailureResponse(err.Error()))
		} else {
			c.JSON(http.StatusInternalServerError, util.MakeFailureResponse(err.Error()))
		}
		return
	}
	c.JSON(http.StatusCreated, util.MakeSuccessResponse("OK"))
}

func RemoveNode(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	id := c.Param("id")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		c.JSON(http.StatusBadRequest, util.MakeFailureResponse(err.Error()))
		return
	}

	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	if err := stor.RemoveNode(ns, cluster, shard, id); err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeNoExists {
			c.JSON(http.StatusNotFound, util.MakeFailureResponse(err.Error()))
		} else {
			c.JSON(http.StatusInternalServerError, util.MakeFailureResponse(err.Error()))
		}
		return
	}
	c.JSON(http.StatusOK, util.MakeSuccessResponse("OK"))
}

func FailoverNode(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	id := c.Param("id")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		c.JSON(http.StatusBadRequest, util.MakeFailureResponse(err.Error()))
		return
	}

	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	nodes, err := stor.ListNodes(ns, cluster, shard)
	if err != nil {
		c.JSON(http.StatusBadRequest, util.MakeFailureResponse(err.Error()))
		return
	}
	var failoverNode *metadata.NodeInfo
	for _, node :=range nodes {
		if strings.HasPrefix(node.ID, id) {
			failoverNode = &node 
			break
		}
	}
	if failoverNode == nil {
		c.JSON(http.StatusBadRequest, util.MakeFailureResponse(metadata.ErrNodeNoExists.Error()))
		return
	}

	fover := c.MustGet(consts.ContextKeyFailover).(*failover.Failover)
	err = fover.AddFailoverNode(ns, cluster, shard, *failoverNode, failover.ManualType)
	if err != nil {
		c.JSON(http.StatusBadRequest, util.MakeFailureResponse(err.Error()))
		return
	}
	c.JSON(http.StatusOK, util.MakeSuccessResponse("OK"))
}
