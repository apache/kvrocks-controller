package handlers

import (
	"net/http"

	"github.com/KvrocksLabs/kvrocks-controller/consts"
	"github.com/KvrocksLabs/kvrocks-controller/metadata/memory"

	"github.com/KvrocksLabs/kvrocks-controller/metadata"
	"github.com/gin-gonic/gin"
)

func ListNode(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	shard := c.Param("shard")

	storage := c.MustGet(consts.ContextKeyStorage).(*memory.MemStorage)
	nodes, err := storage.ListNodes(ns, cluster, shard)
	if err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeNoExists {
			c.JSON(http.StatusNotFound, gin.H{"err": err.Error()})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"err": err.Error()})
		}
		return
	}
	c.JSON(http.StatusOK, gin.H{"nodes": nodes})
}

func CreateNode(c *gin.Context) {
	var nodeInfo metadata.NodeInfo
	nodeInfo.ID = c.Param("id")
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
	if err := storage.CreateNode(ns, cluster, shard, &nodeInfo); err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeExisted {
			c.JSON(http.StatusConflict, gin.H{"err": err.Error()})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"err": err.Error()})
		}
		return
	}
	c.JSON(http.StatusCreated, gin.H{"status": "created"})
}

func RemoveNode(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	shard := c.Param("shard")
	id := c.Param("id")
	storage := c.MustGet(consts.ContextKeyStorage).(*memory.MemStorage)
	if err := storage.RemoveNode(ns, cluster, shard, id); err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeNoExists {
			c.JSON(http.StatusNotFound, gin.H{"err": err.Error()})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"err": err.Error()})
		}
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
