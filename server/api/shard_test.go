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
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"

	"github.com/apache/kvrocks-controller/config"
	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/controller"
	"github.com/apache/kvrocks-controller/server/middleware"
	"github.com/apache/kvrocks-controller/store"
	"github.com/apache/kvrocks-controller/store/engine"
)

func TestShardBasics(t *testing.T) {
	ns := "test-ns"
	clusterName := "test-cluster"
	handler := &ShardHandler{s: store.NewClusterStore(engine.NewMock())}

	// create a test cluster
	shard := store.NewShard()
	shard.SlotRanges = []store.SlotRange{{Start: 0, Stop: 16383}}
	shard.Nodes = []store.Node{store.NewClusterNode("127.0.0.1:1234", "")}

	clusterInfo := &store.Cluster{
		Name:   clusterName,
		Shards: []*store.Shard{shard},
	}
	clusterInfo.Version.Store(1)

	err := handler.s.CreateCluster(context.Background(), ns, clusterInfo)
	require.NoError(t, err)

	runCreate := func(t *testing.T, expectedStatusCode int) {
		var req struct {
			Nodes []string `json:"nodes"`
		}
		req.Nodes = []string{"127.0.0.1:1235", "127.0.0.1:1236"}

		recorder := httptest.NewRecorder()
		ctx := GetTestContext(recorder)
		ctx.Set(consts.ContextKeyStore, handler.s)
		ctx.Params = []gin.Param{{Key: "namespace", Value: ns}, {Key: "cluster", Value: clusterName}}
		body, err := json.Marshal(req)
		require.NoError(t, err)
		ctx.Request.Body = io.NopCloser(bytes.NewBuffer(body))

		middleware.RequiredCluster(ctx)
		require.Equal(t, http.StatusOK, recorder.Code)
		handler.Create(ctx)
		require.Equal(t, expectedStatusCode, recorder.Code)
	}

	runRemove := func(t *testing.T, shardIndex, expectedStatusCode int) {
		recorder := httptest.NewRecorder()
		ctx := GetTestContext(recorder)
		ctx.Set(consts.ContextKeyStore, handler.s)
		ctx.Params = []gin.Param{
			{Key: "namespace", Value: ns},
			{Key: "cluster", Value: clusterName},
			{Key: "shard", Value: strconv.Itoa(shardIndex)},
		}

		middleware.RequiredClusterShard(ctx)
		require.Equal(t, http.StatusOK, recorder.Code)
		handler.Remove(ctx)
		require.Equal(t, expectedStatusCode, recorder.Code)
	}

	t.Run("create shard", func(t *testing.T) {
		runCreate(t, http.StatusCreated)
	})

	t.Run("get shard", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		ctx := GetTestContext(recorder)
		ctx.Set(consts.ContextKeyStore, handler.s)
		ctx.Params = []gin.Param{
			{Key: "namespace", Value: ns},
			{Key: "cluster", Value: clusterName},
			{Key: "shard", Value: "1"},
		}

		middleware.RequiredClusterShard(ctx)
		require.Equal(t, http.StatusOK, recorder.Code)
		handler.Get(ctx)
		require.Equal(t, http.StatusOK, recorder.Code)

		var rsp struct {
			Data struct {
				Shard *store.Shard `json:"shard"`
			} `json:"data"`
		}
		err := json.Unmarshal(recorder.Body.Bytes(), &rsp)
		require.NoError(t, err)
		require.Len(t, rsp.Data.Shard.Nodes, 2)

		var nodeAddrs []string
		for _, node := range rsp.Data.Shard.Nodes {
			nodeAddrs = append(nodeAddrs, node.Addr())
		}
		require.ElementsMatch(t, []string{"127.0.0.1:1235", "127.0.0.1:1236"}, nodeAddrs)
		require.Nil(t, rsp.Data.Shard.MigratingSlot)
		require.EqualValues(t, -1, rsp.Data.Shard.TargetShardIndex)
	})

	t.Run("list shards", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		ctx := GetTestContext(recorder)
		ctx.Set(consts.ContextKeyStore, handler.s)
		ctx.Params = []gin.Param{{Key: "namespace", Value: ns}, {Key: "cluster", Value: clusterName}}

		middleware.RequiredCluster(ctx)
		require.Equal(t, http.StatusOK, recorder.Code)
		handler.List(ctx)
		require.Equal(t, http.StatusOK, recorder.Code)

		var rsp struct {
			Data struct {
				Shards []*store.Shard `json:"shards"`
			} `json:"data"`
		}
		err := json.Unmarshal(recorder.Body.Bytes(), &rsp)
		require.NoError(t, err)
		require.Len(t, rsp.Data.Shards, 2)
	})

	t.Run("remove shard", func(t *testing.T) {
		// shard 0 is servicing
		runRemove(t, 0, http.StatusBadRequest)
		runRemove(t, 1, http.StatusNoContent)
	})
}

func TestClusterFailover(t *testing.T) {
	ns := "test-ns"
	clusterName := "test-cluster-failover"
	clusterStore := store.NewClusterStore(engine.NewMock())
	handler := &ShardHandler{s: clusterStore}
	cluster, err := store.NewCluster(clusterName, []string{"127.0.0.1:7770", "127.0.0.1:7771"}, 2)
	require.NoError(t, err)
	node0, _ := cluster.Shards[0].Nodes[0].(*store.ClusterNode)
	node1, _ := cluster.Shards[0].Nodes[1].(*store.ClusterNode)
	masterClient := redis.NewClient(&redis.Options{Addr: node0.Addr()})
	slaveClient := redis.NewClient(&redis.Options{Addr: node1.Addr()})

	ctx := context.Background()

	require.NoError(t, cluster.Reset(ctx))
	require.NoError(t, cluster.SyncToNodes(ctx))
	defer func() {
		require.NoError(t, cluster.Reset(ctx))
	}()

	ctrl, err := controller.New(clusterStore, &config.ControllerConfig{
		FailOver: &config.FailOverConfig{MaxPingCount: 3, PingIntervalSeconds: 3},
	})
	require.NoError(t, err)
	require.NoError(t, ctrl.Start(ctx))
	ctrl.WaitForReady()
	defer ctrl.Close()

	runFailover := func(t *testing.T, shardIndex, expectedStatusCode int) {
		recorder := httptest.NewRecorder()
		ctx := GetTestContext(recorder)
		ctx.Set(consts.ContextKeyStore, handler.s)
		ctx.Params = []gin.Param{
			{Key: "namespace", Value: ns},
			{Key: "cluster", Value: clusterName},
			{Key: "shard", Value: strconv.Itoa(shardIndex)},
		}

		middleware.RequiredClusterShard(ctx)
		require.Equal(t, http.StatusOK, recorder.Code)
		handler.Failover(ctx)
		require.Equal(t, expectedStatusCode, recorder.Code)
	}

	t.Run("failover is good", func(t *testing.T) {
		require.NoError(t, handler.s.CreateCluster(ctx, ns, cluster))
		require.Eventually(t, func() bool {
			// Confirm that the cluster info has been synced to each node
			clusterInfo, err := node0.GetClusterInfo(ctx)
			if err != nil {
				return false
			}
			return clusterInfo.CurrentEpoch >= 1
		}, 10*time.Second, 100*time.Millisecond)
		require.NoError(t, masterClient.Set(ctx, "my_key", 100, 0).Err())
		require.Eventually(t, func() bool {
			info := strings.Split(slaveClient.Info(ctx).Val(), "\r\n")

			var role string
			sequence := 0
			for _, line := range info {
				kv := strings.Split(line, ":")
				if len(kv) < 2 {
					continue
				}
				if kv[0] == "role" {
					role = kv[1]
				}
				if kv[0] == "sequence" {
					sequence, err = strconv.Atoi(kv[1])
				}
			}
			return role == "slave" && sequence > 0
		}, 30*time.Second, 100*time.Millisecond)

		runFailover(t, 0, http.StatusOK)
		require.NoError(t, slaveClient.FlushAll(ctx).Err())
	})

	t.Run("cluster topology is good", func(t *testing.T) {
		ctx := context.Background()
		gotCluster, err := handler.s.GetCluster(ctx, ns, clusterName)
		require.NoError(t, err)
		require.EqualValues(t, 2, gotCluster.Version.Load())
		require.Len(t, gotCluster.Shards, 1)
		for _, node := range gotCluster.Shards[0].Nodes {
			if node.ID() == node0.ID() {
				// become slave now
				require.False(t, node.IsMaster())
			} else {
				require.True(t, node.IsMaster())
			}
		}

		// sync cluster info to each node
		require.NoError(t, node0.SyncClusterInfo(ctx, gotCluster))
		require.NoError(t, node1.SyncClusterInfo(ctx, gotCluster))

		clusterNodeInfo0, err := node0.GetClusterNodeInfo(ctx)
		require.NoError(t, err)
		require.EqualValues(t, "slave", clusterNodeInfo0.Role)
		clusterNodeInfo1, err := node1.GetClusterNodeInfo(ctx)
		require.NoError(t, err)
		require.EqualValues(t, "master", clusterNodeInfo1.Role)
	})

	t.Run("failover with invalid node id", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		ctx := GetTestContext(recorder)
		ctx.Set(consts.ContextKeyStore, handler.s)
		ctx.Params = []gin.Param{
			{Key: "namespace", Value: ns},
			{Key: "cluster", Value: clusterName},
			{Key: "shard", Value: "0"},
		}
		ctx.Request.Body = io.NopCloser(bytes.NewBufferString(`{"preferred_node_id": "1234567890"}`))
		middleware.RequiredClusterShard(ctx)
		require.Equal(t, http.StatusOK, recorder.Code)
	})
}
