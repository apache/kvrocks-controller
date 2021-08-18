package handlers

import (
	"net/http"

	"github.com/KvrocksLabs/kvrocks-controller/consts"
	"github.com/KvrocksLabs/kvrocks-controller/metadata"
	"github.com/KvrocksLabs/kvrocks-controller/storage/memory"
	"github.com/gin-gonic/gin"
)

func ListShard(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")

	storage := c.MustGet(consts.ContextKeyStorage).(*memory.MemStorage)
	shards, err := storage.ListShard(ns, cluster)
	if err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeNoExists {
			c.JSON(http.StatusNotFound, gin.H{"err": err.Error()})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"err": err.Error()})
		}
		return
	}
	c.JSON(http.StatusOK, gin.H{"shards": shards})
}

func GetShard(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	shard := c.Param("shard")

	storage := c.MustGet(consts.ContextKeyStorage).(*memory.MemStorage)
	s, err := storage.GetShard(ns, cluster, shard)
	if err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeNoExists {
			c.JSON(http.StatusNotFound, gin.H{"err": err.Error()})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"err": err.Error()})
		}
		return
	}
	c.JSON(http.StatusOK, gin.H{"shard": s})
}

func CreateShard(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	shard := c.Param("shard")

	storage := c.MustGet(consts.ContextKeyStorage).(*memory.MemStorage)
	if err := storage.CreateShard(ns, cluster, shard); err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeExisted {
			c.JSON(http.StatusConflict, gin.H{"err": err.Error()})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"err": err.Error()})
		}
		return
	}
	c.JSON(http.StatusCreated, gin.H{"status": "created"})
}

func RemoveShard(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	shard := c.Param("shard")

	storage := c.MustGet(consts.ContextKeyStorage).(*memory.MemStorage)
	if err := storage.RemoveShard(ns, cluster, shard); err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeNoExists {
			c.JSON(http.StatusNotFound, gin.H{"err": err.Error()})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"err": err.Error()})
		}
		return
	}
	c.JSON(http.StatusCreated, gin.H{"status": "created"})
}

func AddShardSlots(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	shard := c.Param("shard")

	var payload struct {
		Slots []string `json:"slots" validate:"required"`
	}
	if err := c.BindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"err": err.Error()})
		return
	}
	slotRanges := make([]metadata.SlotRange, len(payload.Slots))
	for i, slot := range payload.Slots {
		slotRange, err := metadata.ParseSlotRange(slot)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"err": err.Error()})
			return
		}
		slotRanges[i] = *slotRange
	}

	storage := c.MustGet(consts.ContextKeyStorage).(*memory.MemStorage)
	if err := storage.AddShardSlots(ns, cluster, shard, slotRanges); err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeNoExists {
			c.JSON(http.StatusNotFound, gin.H{"err": err.Error()})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"err": err.Error()})
		}
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
