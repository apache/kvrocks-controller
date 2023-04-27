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

package storage

import (
	"context"
	"sort"
	"testing"

	"github.com/RocksLabs/kvrocks_controller/metadata"
	"github.com/RocksLabs/kvrocks_controller/storage/persistence/etcd"
	"github.com/RocksLabs/kvrocks_controller/util"
	"github.com/stretchr/testify/require"
)

var storage *Storage

func requireStorage(t *testing.T) {
	if storage != nil {
		return
	}
	e, err := etcd.New("test-storage", "/test/storage/leader", []string{"127.0.0.1:2379"})
	require.NoError(t, err)
	storage, err = NewStorage(e)
	require.NoError(t, err)
}

func TestStorage_Namespace(t *testing.T) {
	requireStorage(t)

	ctx := context.Background()
	namespaces := []string{util.RandString(40), util.RandString(40), util.RandString(40)}
	sort.Strings(namespaces)
	for _, namespace := range namespaces {
		require.NoError(t, storage.CreateNamespace(ctx, namespace))
	}

	for _, namespace := range namespaces {
		exists, err := storage.IsNamespaceExists(ctx, namespace)
		require.NoError(t, err)
		require.True(t, exists)
	}

	require.Equal(t, metadata.ErrEntryExisted, storage.CreateNamespace(ctx, namespaces[0]))

	gotNamespaces, err := storage.ListNamespace(ctx)
	require.NoError(t, err)

	for _, namespace := range namespaces {
		require.Contains(t, gotNamespaces, namespace)
		require.NoError(t, storage.RemoveNamespace(ctx, namespace))
	}
}

func TestStorage_Cluster(t *testing.T) {
	requireStorage(t)
	ns := util.RandString(10)
	clusterName := util.RandString(10)
	ctx := context.Background()

	newClusterInfo := &metadata.Cluster{
		Name: clusterName,
		Shards: []metadata.Shard{
			{
				Nodes: []metadata.NodeInfo{
					{ID: util.RandString(40), Addr: "1.1.1.1:6379", Role: metadata.RoleMaster},
				},
				SlotRanges: []metadata.SlotRange{
					{Start: 0, Stop: 5000},
				},
			},
		},
	}
	err := storage.CreateCluster(ctx, ns, newClusterInfo)

	require.NoError(t, err)
	exists, err := storage.IsClusterExists(ctx, ns, clusterName)
	require.NoError(t, err)
	require.True(t, exists)
	clusterInfo, err := storage.GetClusterInfo(ctx, ns, clusterName)
	require.Equal(t, newClusterInfo, clusterInfo)
	require.NoError(t, err)
	require.NoError(t, storage.RemoveCluster(ctx, ns, clusterName))
	exists, err = storage.IsClusterExists(ctx, ns, clusterName)
	require.NoError(t, err)
	require.False(t, exists)
}
