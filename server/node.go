package server

import (
	"strconv"
	"time"

	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/metadata"
	"github.com/apache/kvrocks-controller/storage"
	"github.com/apache/kvrocks-controller/util"
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
	if c.GetHeader(consts.HeaderDontDetectHost) != "true" {
		if err := util.DetectClusterNode(c, &nodeInfo); err != nil {
			responseBadRequest(c, err)
			return
		}
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
