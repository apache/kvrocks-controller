package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/KvrocksLabs/kvrocks_controller/consts"
	"github.com/KvrocksLabs/kvrocks_controller/failover"
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/migrate"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
)

func ListCluster(c *gin.Context) {
	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	namespace := c.Param("namespace")
	clusters, err := stor.ListCluster(namespace)
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, clusters)
}

func GetCluster(c *gin.Context) {
	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	namespace := c.Param("namespace")
	clusterName := c.Param("cluster")
	cluster, err := stor.GetClusterCopy(namespace, clusterName)
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, cluster)
}

func CreateCluster(c *gin.Context) {
	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	namespace := c.Param("namespace")

	var req CreateClusterRequest
	if err := c.BindJSON(&req); err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}
	if err := req.validate(); err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}
	shards := make([]metadata.Shard, len(req.Shards))
	slotRanges := metadata.SpiltSlotRange(len(req.Shards))
	for i, createShard := range req.Shards {
		shard, err := createShard.toShard()
		if err != nil {
			responseErrorWithCode(c, http.StatusBadRequest, err.Error())
			return
		}
		shard.SlotRanges = append(shard.SlotRanges, slotRanges[i])
		shards[i] = *shard
	}

	if err := stor.CreateCluster(namespace, req.Cluster, &metadata.Cluster{Shards: shards}); err != nil {
		responseError(c, err)
		return
	}
	responseCreated(c, "OK")
}

func RemoveCluster(c *gin.Context) {
	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	namespace := c.Param("namespace")
	cluster := c.Param("cluster")
	if err := stor.RemoveCluster(namespace, cluster); err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, "OK")
}

func GetFailOverTasks(c *gin.Context) {
	namespace := c.Param("namespace")
	cluster := c.Param("cluster")
	typ := c.Param("type")
	failover, _ := c.MustGet(consts.ContextKeyFailover).(*failover.FailOver)
	tasks, err := failover.GetTasks(namespace, cluster, typ)
	if err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}
	responseOK(c, tasks)
}

func GetMigratingTasks(c *gin.Context) {
	namespace := c.Param("namespace")
	cluster := c.Param("cluster")
	typ := c.Param("type")

	migr := c.MustGet(consts.ContextKeyMigrate).(*migrate.Migrate)
	tasks, err := migr.GetMigrateTasks(namespace, cluster, typ)
	if err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}
	responseOK(c, tasks)
}
