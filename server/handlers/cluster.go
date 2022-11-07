package handlers

import (
	"net/http"

	"github.com/KvrocksLabs/kvrocks_controller/controller/failover"
	"github.com/KvrocksLabs/kvrocks_controller/controller/migrate"

	"github.com/gin-gonic/gin"

	"github.com/KvrocksLabs/kvrocks_controller/consts"
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
)

func ListCluster(c *gin.Context) {
	storage := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	namespace := c.Param("namespace")
	clusters, err := storage.ListCluster(namespace)
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, clusters)
}

func GetCluster(c *gin.Context) {
	storage := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	namespace := c.Param("namespace")
	clusterName := c.Param("cluster")
	cluster, err := storage.GetClusterInfo(namespace, clusterName)
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, cluster)
}

func CreateCluster(c *gin.Context) {
	storage := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
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

	err := storage.CreateCluster(namespace, req.Cluster, &metadata.Cluster{Shards: shards})
	if err != nil {
		responseError(c, err)
		return
	}
	responseCreated(c, "Created")
}

func RemoveCluster(c *gin.Context) {
	storage := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	namespace := c.Param("namespace")
	cluster := c.Param("cluster")
	err := storage.RemoveCluster(namespace, cluster)
	if err != nil {
		responseError(c, err)
		return
	}
	responseCreated(c, "OK")
}

func GetFailOverTasks(c *gin.Context) {
	namespace := c.Param("namespace")
	cluster := c.Param("cluster")
	typ := c.Param("type")
	failover, _ := c.MustGet(consts.ContextKeyFailover).(*failover.FailOver)
	tasks, err := failover.GetTasks(namespace, cluster, typ)
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, tasks)
}

func GetMigratingTasks(c *gin.Context) {
	namespace := c.Param("namespace")
	cluster := c.Param("cluster")
	typ := c.Param("type")

	migration := c.MustGet(consts.ContextKeyMigrate).(*migrate.Migrate)
	tasks, err := migration.GetMigrateTasks(namespace, cluster, typ)
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, tasks)
}
