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
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"

	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/stretchr/testify/require"
)

func TestShard(t *testing.T) {
	ns := uuid.NewString()
	clusterName := uuid.NewString()
	shardURI := fmt.Sprintf("%s/%s/clusters/%s/shards", "/api/v1/namespaces", ns, clusterName)

	createTestNamespace(t, ns)
	createTestCluster(t, ns, clusterName)

	t.Run("Create Shard", func(t *testing.T) {
		server, newRequest := newTestServer()
		require.NotEmpty(t, server)
		var req struct {
			Nodes []string `json:"nodes"`
		}
		req.Nodes = []string{"1.1.1.4"}
		newRequest().
			Post(shardURI).
			JSON(req).
			Expect(t).
			Status(http.StatusCreated).
			End()
	})

	t.Run("Get shard", func(t *testing.T) {
		server, newRequest := newTestServer()
		require.NotEmpty(t, server)

		var result struct {
			Data struct {
				Shard metadata.Shard `json:"shard"`
			}
		}
		newRequest().
			Get(shardURI + "/3").
			Expect(t).
			Status(http.StatusOK).
			End().
			JSON(&result)
		require.Equal(t, 1, len(result.Data.Shard.Nodes))
	})

	t.Run("List shards", func(t *testing.T) {
		server, newRequest := newTestServer()
		require.NotEmpty(t, server)

		var result struct {
			Data struct {
				Shards []metadata.Shard `json:"shards"`
			}
		}
		newRequest().
			Get(shardURI).
			Expect(t).
			Status(http.StatusOK).
			End().
			JSON(&result)
		require.Equal(t, 4, len(result.Data.Shards))
	})

	t.Run("Delete shard", func(t *testing.T) {
		server, newRequest := newTestServer()
		require.NotEmpty(t, server)
		newRequest().
			Delete(shardURI + "/3").
			Expect(t).
			Status(http.StatusNoContent).
			End()
	})
}
