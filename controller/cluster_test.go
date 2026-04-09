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

package controller

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/store"
	"github.com/apache/kvrocks-controller/store/engine"
)

type MockClusterStore struct {
	*store.ClusterStore

	mu       sync.Mutex
	clusters map[string]*store.Cluster
}

func NewMockClusterStore() *MockClusterStore {
	return &MockClusterStore{
		ClusterStore: store.NewClusterStore(engine.NewMock()),
		clusters:     make(map[string]*store.Cluster),
	}
}

func (s *MockClusterStore) CreateCluster(ctx context.Context, ns string, cluster *store.Cluster) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clusters[cluster.Name] = cluster
	return nil
}

func (s *MockClusterStore) GetCluster(ctx context.Context, ns, cluster string) (*store.Cluster, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.clusters[cluster]
	if !ok {
		return nil, consts.ErrNotFound
	}
	return c, nil
}

func (s *MockClusterStore) UpdateCluster(ctx context.Context, ns string, cluster *store.Cluster) error {
	cluster.Version.Add(1)
	return s.SetCluster(ctx, ns, cluster)
}

func (s *MockClusterStore) SetCluster(ctx context.Context, ns string, cluster *store.Cluster) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clusters[cluster.Name] = cluster
	return nil
}

func (s *MockClusterStore) RemoveCluster(ctx context.Context, ns, cluster string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.clusters, cluster)
	return nil
}

func TestCluster_FailureCount(t *testing.T) {
	ctx := context.Background()
	ns := "test-ns"
	clusterName := "test-clusterName"

	s := NewMockClusterStore()
	mockNode0 := store.NewClusterMockNode()
	mockNode0.SetRole(store.RoleMaster)
	mockNode0.Sequence = 104
	mockNode1 := store.NewClusterMockNode()
	mockNode1.SetRole(store.RoleSlave)
	mockNode1.Sequence = 102
	mockNode2 := store.NewClusterMockNode()
	mockNode2.SetRole(store.RoleSlave)
	mockNode2.Sequence = 103
	mockNode3 := store.NewClusterMockNode()
	mockNode3.SetRole(store.RoleSlave)
	mockNode3.Sequence = 101

	clusterInfo := &store.Cluster{
		Name: clusterName,
		Shards: []*store.Shard{{
			Nodes: []store.Node{
				mockNode0, mockNode1, mockNode2, mockNode3,
			},
			SlotRanges:       []store.SlotRange{{Start: 0, Stop: 16383}},
			MigratingSlot:    &store.MigratingSlot{IsMigrating: false},
			TargetShardIndex: -1,
		}},
	}
	clusterInfo.Version.Store(1)

	require.NoError(t, s.CreateCluster(ctx, ns, clusterInfo))

	cluster := &ClusterChecker{
		clusterStore: s,
		namespace:    ns,
		clusterName:  clusterName,
		options: ClusterCheckOptions{
			pingInterval:    time.Second,
			maxFailureCount: 3,
		},
		failureCounts: make(map[string]int64),
		syncCh:        make(chan struct{}, 1),
	}

	require.EqualValues(t, 1, clusterInfo.Version.Load())
	for i := int64(0); i < cluster.options.maxFailureCount-1; i++ {
		require.EqualValues(t, i+1, cluster.increaseFailureCount(0, mockNode2))
	}
	for i := int64(0); i < cluster.options.maxFailureCount; i++ {
		require.EqualValues(t, i+1, cluster.increaseFailureCount(0, mockNode0))
	}
	require.False(t, mockNode0.IsMaster())
	// mockNode2 should become the new master since its sequence is the largest
	require.True(t, mockNode2.IsMaster())
	require.EqualValues(t, 2, clusterInfo.Version.Load())

	require.EqualValues(t, 0, cluster.failureCounts[mockNode2.Addr()])
	require.True(t, mockNode2.IsMaster())

	// Slave failure count keeps increasing; at threshold the slave is auto-marked as failed.
	for i := int64(0); i < cluster.options.maxFailureCount*2; i++ {
		require.EqualValues(t, i+1, cluster.increaseFailureCount(0, mockNode3))
	}
	require.True(t, mockNode3.Failed())
	require.EqualValues(t, 3, clusterInfo.Version.Load())
	cluster.resetFailureCount(mockNode3.ID())
	require.EqualValues(t, 0, cluster.failureCounts[mockNode3.ID()])
}

func TestCluster_SlaveFailureAutoOffline(t *testing.T) {
	ctx := context.Background()
	ns := "test-ns"
	clusterName := "test-slave-offline"

	s := NewMockClusterStore()
	mockMaster := store.NewClusterMockNode()
	mockMaster.SetRole(store.RoleMaster)
	mockMaster.Sequence = 100

	mockSlave1 := store.NewClusterMockNode()
	mockSlave1.SetRole(store.RoleSlave)
	mockSlave1.Sequence = 90

	mockSlave2 := store.NewClusterMockNode()
	mockSlave2.SetRole(store.RoleSlave)
	mockSlave2.Sequence = 80

	clusterInfo := &store.Cluster{
		Name: clusterName,
		Shards: []*store.Shard{{
			Nodes:            []store.Node{mockMaster, mockSlave1, mockSlave2},
			SlotRanges:       []store.SlotRange{{Start: 0, Stop: 16383}},
			MigratingSlot:    &store.MigratingSlot{IsMigrating: false},
			TargetShardIndex: -1,
		}},
	}
	clusterInfo.Version.Store(1)
	require.NoError(t, s.CreateCluster(ctx, ns, clusterInfo))

	checker := &ClusterChecker{
		clusterStore: s,
		namespace:    ns,
		clusterName:  clusterName,
		options: ClusterCheckOptions{
			pingInterval:    time.Second,
			maxFailureCount: 3,
		},
		failureCounts: make(map[string]int64),
		syncCh:        make(chan struct{}, 1),
	}

	// Slave should not be marked as failed before reaching threshold
	require.False(t, mockSlave1.Failed())
	for i := int64(0); i < checker.options.maxFailureCount-1; i++ {
		checker.increaseFailureCount(0, mockSlave1)
	}
	require.False(t, mockSlave1.Failed())
	require.EqualValues(t, 1, clusterInfo.Version.Load())

	// Slave should be marked as failed when reaching threshold
	checker.increaseFailureCount(0, mockSlave1)
	require.True(t, mockSlave1.Failed())
	require.EqualValues(t, 2, clusterInfo.Version.Load())

	// Subsequent failures should not trigger another update (already failed)
	checker.increaseFailureCount(0, mockSlave1)
	require.True(t, mockSlave1.Failed())
	require.EqualValues(t, 2, clusterInfo.Version.Load())

	// Other slaves are not affected
	require.False(t, mockSlave2.Failed())

	// Master should not be affected by slave offline logic
	require.True(t, mockMaster.IsMaster())
	require.False(t, mockMaster.Failed())
}

func TestCluster_LoadAndProbe(t *testing.T) {
	ctx := context.Background()
	ns := "test-ns"
	clusterName := "test-clusterProbe"
	cluster, err := store.NewCluster(clusterName, []string{"127.0.0.1:7770", "127.0.0.1:7771"}, 2)
	require.NoError(t, err)

	nodes := make([]*store.ClusterNode, 0)
	for _, shard := range cluster.Shards {
		for _, node := range shard.Nodes {
			clusterNode, _ := node.(*store.ClusterNode)
			nodes = append(nodes, clusterNode)
		}
	}
	require.NoError(t, cluster.Reset(ctx))
	defer func() {
		require.NoError(t, cluster.Reset(ctx))
	}()

	s := NewMockClusterStore()
	require.NoError(t, s.CreateCluster(ctx, ns, cluster))

	clusterProbe := NewClusterChecker(s, ns, clusterName)
	clusterProbe.WithPingInterval(100 * time.Millisecond)
	clusterProbe.Start()
	defer clusterProbe.Close()

	ticker := time.NewTicker(400 * time.Millisecond)
	defer ticker.Stop()
	<-ticker.C

	for _, node := range nodes {
		info, err := node.GetClusterInfo(ctx)
		require.NoError(t, err)
		require.EqualValues(t, 1, info.CurrentEpoch)
	}
	require.NoError(t, s.UpdateCluster(ctx, ns, cluster))

	<-ticker.C
	// should sync the clusterName info
	for _, node := range nodes {
		info, err := node.GetClusterInfo(ctx)
		require.NoError(t, err)
		require.EqualValues(t, 2, info.CurrentEpoch)
	}
	require.NoError(t, s.UpdateCluster(ctx, ns, cluster))
	clusterProbe.sendSyncEvent()
	<-ticker.C
	for _, node := range nodes {
		info, err := node.GetClusterInfo(ctx)
		require.NoError(t, err)
		require.EqualValues(t, 3, info.CurrentEpoch)
	}
}

func TestCluster_MigrateSlot(t *testing.T) {
	ctx := context.Background()
	ns := "test-ns"
	clusterName := "test-clusterProbe"
	cluster, err := store.NewCluster(clusterName, []string{"127.0.0.1:7770", "127.0.0.1:7771"}, 1)
	require.NoError(t, err)

	require.NoError(t, cluster.Reset(ctx))
	require.NoError(t, cluster.SyncToNodes(ctx))
	defer func() {
		require.NoError(t, cluster.Reset(ctx))
	}()
	slotRange, err := store.NewSlotRange(0, 0)
	require.NoError(t, err)
	require.NoError(t, cluster.MigrateSlot(ctx, slotRange, 1, false))

	s := NewMockClusterStore()
	require.NoError(t, s.CreateCluster(ctx, ns, cluster))

	clusterProbe := NewClusterChecker(s, ns, clusterName)
	clusterProbe.WithPingInterval(100 * time.Millisecond)
	clusterProbe.Start()
	defer clusterProbe.Close()

	ticker := time.NewTicker(400 * time.Millisecond)
	defer ticker.Stop()
	<-ticker.C
}
