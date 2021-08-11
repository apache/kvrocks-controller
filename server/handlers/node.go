package handlers

import (
	"net/http"

	"github.com/KvrocksLabs/kvrocks-controller/consts"
	"github.com/KvrocksLabs/kvrocks-controller/meta/memory"

	"github.com/KvrocksLabs/kvrocks-controller/meta"
	"github.com/gin-gonic/gin"
)

func CreateNode(c *gin.Context) {
	var nodeInfo *meta.NodeInfo
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
	shard := c.Param("shard")

	storage := c.MustGet(consts.ContextKeyStorage).(*memory.MemStorage)
	if err := storage.CreateNode(ns, cluster, shard, nodeInfo); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"err": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"status": "created"})
}

func ListNode(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	shard := c.Param("shard")

	storage := c.MustGet(consts.ContextKeyStorage).(*memory.MemStorage)
	nodes, err := storage.ListNodes(ns, cluster, shard)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"err": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"nodes": nodes})
}
