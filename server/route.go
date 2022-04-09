package server

import (
	"github.com/KvrocksLabs/kvrocks-controller/consts"
	"github.com/KvrocksLabs/kvrocks-controller/server/handlers"
	"github.com/gin-gonic/gin"
)

func SetupRoute(srv *Server, engine *gin.Engine) {
	engine.Use(func(c *gin.Context) {
		c.Set(consts.ContextKeyStorage, srv.stor)
		c.Next()
	})

	apiV1 := engine.Group("/api/v1/")
	{
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
		}

		shards := clusters.Group("/:cluster/shards")
		{
			shards.GET("", handlers.ListShard)
			shards.GET("/:shard", handlers.GetShard)
			shards.POST("", handlers.CreateShard)
			shards.DELETE("/:shard", handlers.RemoveShard)
			shards.POST("/:shard/slots", handlers.AddShardSlots)
		}

		nodes := shards.Group("/:shard/nodes")
		{
			nodes.GET("", handlers.ListNode)
			nodes.POST("/:id", handlers.CreateNode)
			nodes.DELETE("/:id", handlers.RemoveNode)
		}
	}
}
