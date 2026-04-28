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
package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/store/engine"
)

func TestRegisterSelfAndListActivePeers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := engine.NewMock()

	// Register node-2
	m.SetID("node-2")
	s2 := NewClusterStore(m)
	require.NoError(t, s2.RegisterSelf(ctx, "10.0.0.2:9379"))

	// Register node-3
	m.SetID("node-3")
	s3 := NewClusterStore(m)
	require.NoError(t, s3.RegisterSelf(ctx, "10.0.0.3:9379"))

	// Query from node-1's perspective
	m.SetID("node-1")
	s1 := NewClusterStore(m)
	peers, err := s1.ListActivePeers(ctx)
	require.NoError(t, err)
	require.Len(t, peers, 2)
	addrs := map[string]string{}
	for _, p := range peers {
		addrs[p.ID] = p.HTTPAddr
	}
	assert.Equal(t, "10.0.0.2:9379", addrs["node-2"])
	assert.Equal(t, "10.0.0.3:9379", addrs["node-3"])
	assert.NotContains(t, addrs, "node-1")
}

func TestListActivePeers_Empty(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := engine.NewMock()
	m.SetID("solo")
	s := NewClusterStore(m)
	require.NoError(t, s.RegisterSelf(ctx, "10.0.0.1:9379"))

	peers, err := s.ListActivePeers(ctx)
	require.NoError(t, err)
	assert.Empty(t, peers)
}

// TestRegisterSelf_DeletesKeyOnShutdown verifies Fix 3: when the context passed
// to RegisterSelf is cancelled (clean shutdown), the background goroutine must
// delete the peer key immediately instead of letting it expire via TTL. Without
// the fix, rolling restarts leave a stale key for up to peerTTL (15 s), causing
// VoteCoordinator to treat the dead node as "alive but unreachable" and block
// failover for the entire TTL window.
func TestRegisterSelf_DeletesKeyOnShutdown(t *testing.T) {
	m := engine.NewMock()

	// Register as node-dying.
	m.SetID("node-dying")
	sDying := NewClusterStore(m)
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, sDying.RegisterSelf(ctx, "10.0.0.50:9379"))

	// Confirm the registration is visible from a different node's perspective.
	m.SetID("node-viewer")
	sViewer := NewClusterStore(m)
	peers, err := sViewer.ListActivePeers(context.Background())
	require.NoError(t, err)
	require.Len(t, peers, 1, "peer should be visible before shutdown")

	// Simulate clean shutdown by cancelling the context.
	cancel()

	// The background goroutine should delete the key almost immediately.
	require.Eventually(t,
		func() bool {
			p, e := sViewer.ListActivePeers(context.Background())
			return e == nil && len(p) == 0
		},
		500*time.Millisecond, 10*time.Millisecond,
		"peer key must be deleted on clean shutdown, not left to expire via TTL",
	)
}

func TestListActivePeers_StaleEntryExcluded(t *testing.T) {
	ctx := context.Background()
	m := engine.NewMock()
	m.SetID("viewer")
	s := NewClusterStore(m)

	// Write a peer entry with a timestamp older than peerTTL.
	staleTS := time.Now().Add(-2 * peerTTL).Unix()
	require.NoError(t, m.Set(ctx, peerKeyPrefix+"dead-node",
		[]byte(fmt.Sprintf("10.0.0.99:9379|%d", staleTS))))

	peers, err := s.ListActivePeers(ctx)
	require.NoError(t, err)
	assert.Empty(t, peers, "stale peer must be excluded from quorum")
}

func TestListActivePeers_OldFormatExcluded(t *testing.T) {
	ctx := context.Background()
	m := engine.NewMock()
	m.SetID("viewer")
	s := NewClusterStore(m)

	// Write a peer entry in the old format (no timestamp) — must also be excluded.
	require.NoError(t, m.Set(ctx, peerKeyPrefix+"legacy-node",
		[]byte("10.0.0.50:9379")))

	peers, err := s.ListActivePeers(ctx)
	require.NoError(t, err)
	assert.Empty(t, peers, "legacy format peer (no timestamp) must be excluded")
}

func TestClusterStore(t *testing.T) {
	ctx := context.Background()
	store := NewClusterStore(engine.NewMock())

	t.Run("create/get/list/delete namespace", func(t *testing.T) {
		namespaces := []string{"ns0", "ns1", "ns2"}
		for _, ns := range namespaces {
			err := store.CreateNamespace(ctx, ns)
			require.NoError(t, err)
			require.ErrorIs(t, store.CreateNamespace(ctx, ns), consts.ErrAlreadyExists)
			exists, err := store.ExistsNamespace(ctx, ns)
			require.NoError(t, err)
			require.True(t, exists)
		}

		exists, err := store.ExistsNamespace(ctx, "not-exits-ns")
		require.NoError(t, err)
		require.False(t, exists)

		gotNamespaces, err := store.ListNamespace(ctx)
		require.NoError(t, err)
		require.ElementsMatch(t, namespaces, gotNamespaces)

		for _, ns := range namespaces {
			err := store.RemoveNamespace(ctx, ns)
			require.NoError(t, err)
			exists, err := store.ExistsNamespace(ctx, ns)
			require.NoError(t, err)
			require.False(t, exists)
		}
	})

	t.Run("create/get/list/delete cluster", func(t *testing.T) {
		ns := "ns0"
		cluster0 := &Cluster{Name: "cluster0", Shards: Shards{NewShard()}}
		cluster1 := &Cluster{Name: "cluster1", Shards: Shards{NewShard()}}
		cluster0.Version.Store(2)
		cluster1.Version.Store(3)

		require.NoError(t, store.CreateCluster(ctx, ns, cluster0))
		require.ErrorIs(t, store.CreateCluster(ctx, ns, cluster0), consts.ErrAlreadyExists)
		require.NoError(t, store.CreateCluster(ctx, ns, cluster1))

		gotCluster, err := store.GetCluster(ctx, ns, "cluster0")
		require.NoError(t, err)
		require.Equal(t, cluster0.Name, gotCluster.Name)
		require.Equal(t, cluster0.Version.Load(), gotCluster.Version.Load())

		gotClusters, err := store.ListCluster(ctx, ns)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"cluster0", "cluster1"}, gotClusters)

		require.NoError(t, store.UpdateCluster(ctx, ns, cluster0))
		gotCluster, err = store.GetCluster(ctx, ns, "cluster0")
		require.NoError(t, err)
		require.Equal(t, cluster0.Name, gotCluster.Name)
		require.EqualValues(t, 3, gotCluster.Version.Load())

		for _, name := range []string{"cluster0", "cluster1"} {
			require.NoError(t, store.RemoveCluster(ctx, ns, name))
			_, err = store.GetCluster(ctx, ns, name)
			require.ErrorIs(t, err, consts.ErrNotFound)
		}
	})

	t.Run("check nodes", func(t *testing.T) {
		testCluster, err := NewCluster("test-cluster-another",
			[]string{"127.0.0.1:1111", "127.0.0.1:2222", "127.0.0.1:3333"}, 1)
		require.NoError(t, err)

		require.NoError(t, store.CreateCluster(ctx, "test-ns", testCluster))
		require.NoError(t, store.CheckNewNodes(ctx, []string{"127.0.0.1:4444", "127.0.0.1:5555"}))
		require.NotNil(t, store.CheckNewNodes(ctx, []string{"127.0.0.1:3333", "127.0.0.1:4444"}))
		require.NotNil(t, store.CheckNewNodes(ctx, []string{"127.0.0.1:2222", "127.0.0.1:3333"}))
	})
}
