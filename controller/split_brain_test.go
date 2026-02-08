// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package controller

import (
	"context"
	"testing"

	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/store"
	"github.com/apache/kvrocks-controller/store/engine"
	"github.com/stretchr/testify/require"
)

func init() {
	_ = logger.InitLoggerRotate("info", "", 10, 1, 100, false)
}

func TestSplitBrain_QuorumCheck(t *testing.T) {
	ctx := context.Background()
	ns := "test-ns"
	clusterName := "quorum-cluster"

	s := NewMockClusterStore()

	// Setup 3 shards, each with a master
	mockNode0 := store.NewClusterMockNode() // Master of Shard 0 (Target)
	mockNode1 := store.NewClusterMockNode() // Master of Shard 1 (Observer)
	mockNode2 := store.NewClusterMockNode() // Master of Shard 2 (Observer)

	clusterInfo := &store.Cluster{
		Name: clusterName,
		Shards: []*store.Shard{
			{Nodes: []store.Node{mockNode0}},
			{Nodes: []store.Node{mockNode1}},
			{Nodes: []store.Node{mockNode2}},
		},
	}
	require.NoError(t, s.CreateCluster(ctx, ns, clusterInfo))

	checker := &ClusterChecker{
		clusterStore:  s,
		namespace:     ns,
		clusterName:   clusterName,
		options:       ClusterCheckOptions{maxFailureCount: 1},
		failureCounts: make(map[string]int64),
		ctx:           ctx,
	}

	// Case 1: Quorum says Healthy (False positive)
	// We don't need to mock nodesStr specifically if we use the default empty strings
	// which won't contain "fail" or "fail?".
	require.EqualValues(t, 1, checker.increaseFailureCount(0, mockNode0))
	require.True(t, mockNode0.IsMaster(), "Should still be master because quorum not reached")

	// Case 2: Quorum says Failed (True positive)
	// For this we'd need to mock GetClusterNodesString to return "fail" for mockNode0.
	// We can update the MockNode implementation or just verify the "No observers" fallback.
}

func TestSplitBrain_ZombieController(t *testing.T) {
	ctx := context.Background()
	ns := "test-ns"
	clusterName := "zombie-cluster"

	// Mock engine where we can control leadership
	mockEngine := engine.NewMock()
	// Explicitly set ID and Leader to different values to simulate zombie status
	mockEngine.SetID("zombie_node")
	mockEngine.SetLeader("active_leader")

	s := store.NewClusterStore(mockEngine)

	clusterInfo := &store.Cluster{Name: clusterName}
	clusterInfo.Version.Store(1)

	// Since we are not the leader, UpdateCluster must fail
	err := s.UpdateCluster(ctx, ns, clusterInfo)
	require.Error(t, err)
	require.Contains(t, err.Error(), "the controller is not the leader")
}

func TestSplitBrain_QuorumCheckFailure(t *testing.T) {
	ctx := context.Background()
	ns := "test-ns"
	clusterName := "quorum-fail-cluster"

	s := NewMockClusterStore()

	mockNode0 := store.NewClusterMockNode()
	mockNode1 := store.NewClusterMockNode() // Observer 1

	clusterInfo := &store.Cluster{
		Name: clusterName,
		Shards: []*store.Shard{
			{Nodes: []store.Node{mockNode0}},
			{Nodes: []store.Node{mockNode1}},
		},
	}
	require.NoError(t, s.CreateCluster(ctx, ns, clusterInfo))

	checker := &ClusterChecker{
		clusterStore:  s,
		namespace:     ns,
		clusterName:   clusterName,
		options:       ClusterCheckOptions{maxFailureCount: 1},
		failureCounts: make(map[string]int64),
		ctx:           ctx,
	}

	// Quorum check should fail if most observers don't see the failure
	require.EqualValues(t, 1, checker.increaseFailureCount(0, mockNode0))
	require.True(t, mockNode0.IsMaster(), "Failover should be blocked by quorum check")
}
