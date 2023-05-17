/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package server

import (
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/RocksLabs/kvrocks_controller/consts"
	"github.com/gin-contrib/static"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func (srv *Server) initHandlers() {
	engine := srv.engine
	engine.Use(CollectMetrics, func(c *gin.Context) {
		c.Set(consts.ContextKeyStorage, srv.storage)
		c.Set(consts.ContextKeyMigrate, srv.controller.GetMigrate())
		c.Set(consts.ContextKeyFailover, srv.controller.GetFailOver())
		c.Next()
	}, RedirectIfNotLeader)
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
			namespaces.GET("/:namespace", namespace.Exists)
			namespaces.POST("", namespace.Create)
			namespaces.DELETE("/:namespace", namespace.Remove)
		}

		clusters := namespaces.Group("/:namespace/clusters")
		{
			clusters.Use(requiredNamespace)
			clusters.GET("", cluster.List)
			clusters.GET("/:cluster", cluster.Get)
			clusters.POST("", cluster.Create)
			clusters.DELETE("/:cluster", cluster.Remove)
			clusters.GET("/:cluster/failover/:type", cluster.GetFailOverTasks)
			clusters.GET("/:cluster/migration/:type", cluster.GetMigratingTasks)
		}

		shards := clusters.Group("/:cluster/shards")
		{
			shards.Use(requiredCluster)
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
			nodes.Use(requiredCluster)
			nodes.GET("", node.List)
			nodes.POST("", node.Create)
			nodes.DELETE("/:id", node.Remove)
			nodes.POST("/:id/failover", node.Failover)
		}
	}

	// web service
	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	webDir := path.Join(filepath.Dir(ex), srv.config.Web.Dir)
	engine.Use(static.Serve("/", static.LocalFile(webDir, false)))
	engine.NoRoute(func(c *gin.Context) {
		accept := c.Request.Header.Get("Accept")
		flag := strings.Contains(accept, "text/html")
		if flag {
			content, err := os.ReadFile(path.Join(webDir, "index.html"))
			if (err) != nil {
				c.Writer.WriteHeader(404)
				_, _ = c.Writer.WriteString("Not Found")
				return
			}
			c.Writer.WriteHeader(200)
			c.Writer.Header().Add("Accept", "text/html")
			_, _ = c.Writer.Write((content))
			c.Writer.Flush()
		}
	})
}
