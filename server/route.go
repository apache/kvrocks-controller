package server

import (
	"github.com/KvrocksLabs/kvrocks_controller/consts"
	"github.com/KvrocksLabs/kvrocks_controller/server/handlers"
	"github.com/KvrocksLabs/kvrocks_controller/server/middlewares"
	"github.com/gin-gonic/gin"
)

func SetupRoute(srv *Server, engine *gin.Engine) {
	engine.Use(middlewares.CollectMetrics, func(c *gin.Context) {
		c.Set(consts.ContextKeyStorage, srv.storage)
		c.Set(consts.ContextKeyMigrate, srv.controller.GetMigrate())
		c.Set(consts.ContextKeyFailover, srv.controller.GetFailOver())
		c.Next()
	})

	apiTest := engine.Group("/api/test/")
	{
		controller := apiTest.Group("controller")
		controller.GET("/leader/resign", handlers.LeaderResign)
	}

	apiV1 := engine.Group("/api/v1/")
	{
		controller := apiV1.Group("controller")
		{
			controller.GET("/leader", handlers.Leader)
		}

		namespaces := apiV1.Group("namespaces")
		{
			namespaces.GET("", handlers.ListNamespace)
			namespaces.POST("", handlers.CreateNamespace)
			namespaces.DELETE("/:namespace", handlers.RemoveNamespace)
		}

		clusters := namespaces.Group("/:namespace/clusters")
		{
			clusters.GET("", handlers.ListCluster)
			clusters.GET("/:cluster", handlers.GetCluster)
			clusters.POST("", handlers.CreateCluster)
			clusters.DELETE("/:cluster", handlers.RemoveCluster)
			clusters.GET("/:cluster/failover/:type", handlers.GetFailOverTasks)
			clusters.GET("/:cluster/migration/:type", handlers.GetMigratingTasks)
		}

		shards := clusters.Group("/:cluster/shards")
		{
			shards.GET("", handlers.ListShard)
			shards.GET("/:shard", handlers.GetShard)
			shards.POST("", handlers.CreateShard)
			shards.DELETE("/:shard", handlers.RemoveShard)
			shards.POST("/:shard/slots", handlers.UpdateShardSlots)
			shards.DELETE("/:shard/slots", handlers.UpdateShardSlots)
			shards.POST("/migration/slot_data", handlers.MigrateSlotData)
			shards.POST("/migration/slot_only", handlers.MigrateSlotOnly)
		}

		nodes := shards.Group("/:shard/nodes")
		{
			nodes.GET("", handlers.ListNode)
			nodes.POST("", handlers.CreateNode)
			nodes.DELETE("/:id", handlers.RemoveNode)
			nodes.POST("/:id/failover", handlers.FailoverNode)
		}
	}
}
