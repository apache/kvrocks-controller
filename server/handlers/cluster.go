package handlers

import (
	"net/http"

	"github.com/KvrocksLabs/kvrocks-controller/consts"
	"github.com/KvrocksLabs/kvrocks-controller/metadata"
	"github.com/KvrocksLabs/kvrocks-controller/storage/memory"
	"github.com/gin-gonic/gin"
)

type createClusterRequest struct {
	Shards []createShardRequest `json:"shards"`
}

func (req *createClusterRequest) validate() error {
	return nil
}

func ListCluster(c *gin.Context) {
	storage := c.MustGet(consts.ContextKeyStorage).(*memory.MemStorage)
	namespace := c.Param("namespace")
	clusters, err := storage.ListCluster(namespace)
	if err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeNoExists {
			c.JSON(http.StatusNotFound, gin.H{"err": err.Error()})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"err": err.Error()})
		}
		return
	}
	c.JSON(http.StatusOK, gin.H{"clusters": clusters})
}

func CreateCluster(c *gin.Context) {
	storage := c.MustGet(consts.ContextKeyStorage).(*memory.MemStorage)
	namespace := c.Param("namespace")
	cluster := c.Param("cluster")

	var req createClusterRequest
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"err": err.Error()})
		return
	}
	if err := req.validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"err": err.Error()})
		return
	}
	shards := make([]metadata.Shard, len(req.Shards))
	for i, createShard := range req.Shards {
		shard, err := createShard.toShard()
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"index": i, "err": err.Error()})
			return
		}
		rangeSize := metadata.MaxSlotID / len(req.Shards)
		if i != len(req.Shards)-1 {
			shard.SlotRanges = []metadata.SlotRange{{Start: i * rangeSize, Stop: (i+1)*rangeSize - 1}}
		} else {
			shard.SlotRanges = []metadata.SlotRange{{Start: i * rangeSize, Stop: metadata.MaxSlotID}}
		}
		shards[i] = *shard
	}

	if err := storage.CreateCluster(namespace, cluster, shards); err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeExisted {
			c.JSON(http.StatusConflict, gin.H{"err": err.Error()})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"err": err.Error()})
		}
		return
	}
	c.JSON(http.StatusCreated, gin.H{"status": "created"})
}

func RemoveCluster(c *gin.Context) {
	storage := c.MustGet(consts.ContextKeyStorage).(*memory.MemStorage)
	namespace := c.Param("namespace")
	cluster := c.Param("cluster")
	if err := storage.RemoveCluster(namespace, cluster); err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeNoExists {
			c.JSON(http.StatusNotFound, gin.H{"err": err.Error()})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"err": err.Error()})
		}
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
