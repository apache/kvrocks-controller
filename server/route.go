package server

import (
	"github.com/KvrocksLabs/kvrocks_controller/consts"
	"github.com/KvrocksLabs/kvrocks_controller/server/middlewares"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func (srv *Server) initHandlers() {
	engine := srv.engine
	engine.Use(middlewares.CollectMetrics, func(c *gin.Context) {
		c.Set(consts.ContextKeyStorage, srv.storage)
		c.Set(consts.ContextKeyMigrate, srv.controller.GetMigrate())
		c.Set(consts.ContextKeyFailover, srv.controller.GetFailOver())
		c.Next()
	}, middlewares.RedirectIfNotLeader)
	namespace := &NamespaceHandler{storage: srv.storage}
	cluster := &ClusterHandler{storage: srv.storage}
	shard := &ShardHandler{storage: srv.storage}
	node := &NodeHandler{storage: srv.storage}

	apiTest := engine.Group("/api/test/")
	{
		controller := apiTest.Group("controller")
		controller.GET("/leader/resign", LeaderResign)
	}
	engine.Any("/debug/pprof/*profile", PProf)
	engine.GET("/metrics", gin.WrapH(promhttp.Handler()))

	apiV1 := engine.Group("/api/v1/")
	{
		controller := apiV1.Group("controller")
		{
			controller.GET("/leader", Leader)
		}

		namespaces := apiV1.Group("namespaces")
		{
			namespaces.GET("", namespace.List)
			namespaces.POST("", namespace.Create)
			namespaces.DELETE("/:namespace", namespace.Remove)
		}

		clusters := namespaces.Group("/:namespace/clusters")
		{
			clusters.GET("", cluster.List)
			clusters.GET("/:cluster", cluster.Get)
			clusters.POST("", cluster.Create)
			clusters.DELETE("/:cluster", cluster.Remove)
			clusters.GET("/:cluster/failover/:type", cluster.GetFailOverTasks)
			clusters.GET("/:cluster/migration/:type", cluster.GetMigratingTasks)
		}

		shards := clusters.Group("/:cluster/shards")
		{
			shards.GET("", shard.List)
			shards.GET("/:shard", shard.Get)
			shards.POST("", shard.Create)
			shards.DELETE("/:shard", shard.Remove)
			shards.POST("/:shard/slots", shard.UpdateSlots)
			shards.DELETE("/:shard/slots", shard.UpdateSlots)
			shards.POST("/migration/slot_data", shard.MigrateSlotData)
			shards.POST("/migration/slot_only", shard.MigrateSlotOnly)
		}

		nodes := shards.Group("/:shard/nodes")
		{
			nodes.GET("", node.List)
			nodes.POST("", node.Create)
			nodes.DELETE("/:id", node.Remove)
			nodes.POST("/:id/failover", node.Failover)
		}
	}
}
