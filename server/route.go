package server

import (
	"github.com/KvrocksLabs/kvrocks-controller/consts"
	"github.com/KvrocksLabs/kvrocks-controller/meta/memory"
	"github.com/KvrocksLabs/kvrocks-controller/server/handlers"
	"github.com/gin-gonic/gin"
)

func SetupRoute(engine *gin.Engine) {
	storage := memory.NewMemStorage()
	_ = storage.CreateNamespace("test-ns")
	_ = storage.CreateCluster("test-ns", "test-cluster")
	_ = storage.CreateShard("test-ns", "test-cluster", "test-shard")
	engine.Use(func(c *gin.Context) {
		c.Set(consts.ContextKeyStorage, storage)
		c.Next()
	})

	nodes := engine.Group("/api/v1/:namespace/clusters/:cluster/shards/:shard/nodes")
	{
		nodes.POST("", handlers.CreateNode)
		nodes.GET("", handlers.ListNode)
	}
}
