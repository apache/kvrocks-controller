package handlers

import (
	"fmt"
	"strconv"
	"net/http"

	"github.com/KvrocksLabs/kvrocks-controller/consts"
	"github.com/KvrocksLabs/kvrocks-controller/metadata"
	"github.com/KvrocksLabs/kvrocks-controller/storage"
	"github.com/KvrocksLabs/kvrocks-controller/util"
	"github.com/gin-gonic/gin"
)

func (req *CreateClusterParam) validate() error {
	if len(req.Cluster) == 0 {
		return fmt.Errorf("cluster name should NOT be empty")
	}
	for i, shard := range req.Shards {
		if err := shard.validate(); err != nil {
			return fmt.Errorf("validate shard[%d] err: %w", i, err)
		}
	}
	return nil
}

func ListCluster(c *gin.Context) {
	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	namespace := c.Param("namespace")
	clusters, err := stor.ListCluster(namespace)
	if err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeNoExists {
			c.JSON(http.StatusNotFound, util.MakeFailureResponse(err.Error()))
		} else {
			c.JSON(http.StatusInternalServerError, util.MakeFailureResponse(err.Error()))
		}
		return
	}
	c.JSON(http.StatusOK, util.MakeSuccessResponse(clusters))
}

func GetCluster(c *gin.Context) {
	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	namespace := c.Param("namespace")
	clusterName := c.Param("cluster")
	cluster, err := stor.GetClusterCopy(namespace, clusterName)
	if err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeNoExists {
			c.JSON(http.StatusNotFound, util.MakeFailureResponse(err.Error()))
		} else {
			c.JSON(http.StatusInternalServerError, util.MakeFailureResponse(err.Error()))
		}
		return
	}
	c.JSON(http.StatusOK, util.MakeSuccessResponse(cluster))
}

func CreateCluster(c *gin.Context) {
	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	namespace := c.Param("namespace")

	var req CreateClusterParam
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, util.MakeFailureResponse(err.Error()))
		return
	}
	if err := req.validate(); err != nil {
		c.JSON(http.StatusBadRequest, util.MakeFailureResponse(err.Error()))
		return
	}
	shards := make([]metadata.Shard, len(req.Shards))
	slotRanges := metadata.SpiltSlotRange(len(req.Shards))
	for i, createShard := range req.Shards {
		shard, err := createShard.toShard()
		if err != nil {
			c.JSON(http.StatusBadRequest, util.MakeFailureResponse("index: " + strconv.Itoa(i) + ", err: " + err.Error()))
			return
		}
		shard.SlotRanges = append(shard.SlotRanges, slotRanges[i])
		shards[i] = *shard
	}

	if err := stor.CreateCluster(namespace, req.Cluster, &metadata.Cluster{Shards: shards, }); err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeExisted {
			c.JSON(http.StatusConflict, util.MakeFailureResponse(err.Error()))
		} else {
			c.JSON(http.StatusInternalServerError, util.MakeFailureResponse(err.Error()))
		}
		return
	}
	c.JSON(http.StatusCreated, util.MakeSuccessResponse("OK"))
}

func RemoveCluster(c *gin.Context) {
	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	namespace := c.Param("namespace")
	cluster := c.Param("cluster")
	if err := stor.RemoveCluster(namespace, cluster); err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeNoExists {
			c.JSON(http.StatusNotFound, util.MakeFailureResponse(err.Error()))
		} else {
			c.JSON(http.StatusInternalServerError, util.MakeFailureResponse(err.Error()))
		}
		return
	}
	c.JSON(http.StatusOK, util.MakeSuccessResponse("OK"))
}
