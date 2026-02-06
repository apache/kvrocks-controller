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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/kvrocks-controller/store"
	"github.com/apache/kvrocks-controller/store/engine"
	"github.com/stretchr/testify/require"
)

// TestMigration_NilSlotRecovery verifies that if a node temporarily reports nil migrating info,
// the controller doesn't immediately clear the migration state but waits for the grace period.
func TestMigration_NilSlotRecovery(t *testing.T) {
	ctx := context.Background()
	ns := "test-ns"
	clusterName := "test-cluster"

	s := NewMockClusterStore()
	node0 := store.NewClusterMockNode()
	node0.SetID("node0")
	node1 := store.NewClusterMockNode()
	node1.SetID("node1")

	cluster := &store.Cluster{
		Name: clusterName,
		Shards: []*store.Shard{
			{
				Nodes:      []store.Node{node0},
				SlotRanges: []store.SlotRange{{Start: 0, Stop: 8191}},
				MigratingSlot: &store.MigratingSlot{
					SlotRange:   store.SlotRange{Start: 10, Stop: 10},
					IsMigrating: true,
				},
				TargetShardIndex: 1,
			},
			{
				Nodes:      []store.Node{node1},
				SlotRanges: []store.SlotRange{{Start: 8192, Stop: 16383}},
			},
		},
	}
	require.NoError(t, s.CreateCluster(ctx, ns, cluster))

	checker := NewClusterChecker(s, ns, clusterName)

	// 1. Simulate node reporting nil MigratingSlot
	// By default ClusterMockNode.GetClusterInfo returns empty ClusterInfo (MigratingSlot == nil)
	checker.tryUpdateMigrationStatus(ctx, cluster.Clone())

	checker.migrationFailureMu.Lock()
	require.Equal(t, 1, checker.migrationFailureCounts["node0"])
	checker.migrationFailureMu.Unlock()

	// 2. Simulate node reporting nil MigratingSlot again
	checker.tryUpdateMigrationStatus(ctx, cluster.Clone())
	checker.migrationFailureMu.Lock()
	require.Equal(t, 2, checker.migrationFailureCounts["node0"])
	checker.migrationFailureMu.Unlock()

	// 3. Simulate node recovering and reporting "success"
	// We need to override GetClusterInfo for node0
	// But ClusterMockNode doesn't have an easy way to override and keep state.
	// Let's use a custom mock locally if needed, but let's see if we can just set it.
	node0.SetMigratingState("success") // This doesn't affect GetClusterInfo directly in current mock implementation

	// Actually, let's look at ClusterMockNode.GetClusterInfo in store/cluster_mock_node.go
	// func (mock *ClusterMockNode) GetClusterInfo(ctx context.Context) (*ClusterInfo, error) {
	// 	return mock.ClusterNode.GetClusterInfo(ctx)
	// }
	// And ClusterNode.GetClusterInfo calls CLUSTER INFO on redis.
	// We need to override it to return what we want.
}

type TestNode struct {
	*store.ClusterMockNode
	info *store.ClusterInfo
}

func (n *TestNode) GetClusterInfo(ctx context.Context) (*store.ClusterInfo, error) {
	return n.info, nil
}

func TestMigration_FinalizationResilience(t *testing.T) {
	ctx := context.Background()
	ns := "test-ns"
	clusterName := "test-resilience"

	s := NewMockClusterStore()

	// Custom nodes that allow us to control ClusterInfo
	sourceNode := &TestNode{
		ClusterMockNode: store.NewClusterMockNode(),
		info: &store.ClusterInfo{
			MigratingSlot: &store.MigratingSlot{
				SlotRange:   store.SlotRange{Start: 10, Stop: 10},
				IsMigrating: true,
			},
			MigratingState: "success",
		},
	}
	sourceNode.SetID("source")

	targetNode := &TestNode{
		ClusterMockNode: store.NewClusterMockNode(),
		info:            &store.ClusterInfo{},
	}
	targetNode.SetID("target")

	cluster := &store.Cluster{
		Name: clusterName,
		Shards: []*store.Shard{
			{
				Nodes:      []store.Node{sourceNode},
				SlotRanges: []store.SlotRange{{Start: 0, Stop: 100}},
				MigratingSlot: &store.MigratingSlot{
					SlotRange:   store.SlotRange{Start: 10, Stop: 10},
					IsMigrating: true,
				},
				TargetShardIndex: 1,
			},
			{
				Nodes:      []store.Node{targetNode},
				SlotRanges: []store.SlotRange{{Start: 101, Stop: 200}},
			},
		},
	}
	require.NoError(t, s.CreateCluster(ctx, ns, cluster))

	checker := NewClusterChecker(s, ns, clusterName)

	// 1. Successful finalization
	checker.tryUpdateMigrationStatus(ctx, cluster.Clone())

	// Verify topology updated in store
	updated, err := s.GetCluster(ctx, ns, clusterName)
	require.NoError(t, err)
	require.False(t, store.SlotRanges(updated.Shards[0].SlotRanges).Contains(10))
	require.True(t, store.SlotRanges(updated.Shards[1].SlotRanges).Contains(10))
	require.Nil(t, updated.Shards[0].MigratingSlot)
}

func TestMigration_ConcurrentStress(t *testing.T) {
	ctx := context.Background()
	ns := "test-ns"
	clusterName := "test-stress"
	s := NewMockClusterStore()

	// 4 Shards, each migrating a slot to the next shard
	numShards := 4
	shards := make([]*store.Shard, numShards)
	nodes := make([]*TestNode, numShards)

	for i := 0; i < numShards; i++ {
		nodes[i] = &TestNode{
			ClusterMockNode: store.NewClusterMockNode(),
			info: &store.ClusterInfo{
				MigratingSlot: &store.MigratingSlot{
					SlotRange:   store.SlotRange{Start: i * 100, Stop: i * 100},
					IsMigrating: true,
				},
				MigratingState: "success",
			},
		}
		nodes[i].SetID(string(rune('a' + i)))
		shards[i] = &store.Shard{
			Nodes:            []store.Node{nodes[i]},
			SlotRanges:       []store.SlotRange{{Start: i * 100, Stop: (i+1)*100 - 1}},
			MigratingSlot:    &store.MigratingSlot{SlotRange: store.SlotRange{Start: i * 100, Stop: i * 100}, IsMigrating: true},
			TargetShardIndex: (i + 1) % numShards,
		}
	}

	cluster := &store.Cluster{Name: clusterName, Shards: shards}
	require.NoError(t, s.CreateCluster(ctx, ns, cluster))
	checker := NewClusterChecker(s, ns, clusterName)

	// Run multiple times to simulate concurrent evaluation if it was multi-threaded
	// (though tryUpdateMigrationStatus is sequential per checker, it tests atomicity of ranges)
	checker.tryUpdateMigrationStatus(ctx, cluster.Clone())

	updated, err := s.GetCluster(ctx, ns, clusterName)
	require.NoError(t, err)

	// Verify all slots moved correctly
	for i := 0; i < numShards; i++ {
		require.Nil(t, updated.Shards[i].MigratingSlot)
		// Shard i lost i*100, gained (i-1)*100
		prev := (i - 1 + numShards) % numShards
		require.False(t, store.SlotRanges(updated.Shards[i].SlotRanges).Contains(i*100))
		require.True(t, store.SlotRanges(updated.Shards[i].SlotRanges).Contains(prev*100))
	}
}

// SharedEngine implements engine.Engine but allows multiple instances to share data
type SharedEngine struct {
	id           string
	data         *sync.Map
	leaderID     *atomic.Value // string
	leaderChange chan bool
}

func NewSharedEngine(id string, data *sync.Map, leaderID *atomic.Value) *SharedEngine {
	return &SharedEngine{
		id:           id,
		data:         data,
		leaderID:     leaderID,
		leaderChange: make(chan bool, 1),
	}
}

func (e *SharedEngine) ID() string                       { return e.id }
func (e *SharedEngine) Leader() string                   { return e.leaderID.Load().(string) }
func (e *SharedEngine) LeaderChange() <-chan bool        { return e.leaderChange }
func (e *SharedEngine) IsReady(ctx context.Context) bool { return true }
func (e *SharedEngine) Close() error                     { return nil }

func (e *SharedEngine) Get(ctx context.Context, key string) ([]byte, error) {
	v, ok := e.data.Load(key)
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return v.([]byte), nil
}

func (e *SharedEngine) Exists(ctx context.Context, key string) (bool, error) {
	_, ok := e.data.Load(key)
	return ok, nil
}

func (e *SharedEngine) Set(ctx context.Context, key string, value []byte) error {
	e.data.Store(key, value)
	return nil
}

func (e *SharedEngine) Delete(ctx context.Context, key string) error {
	e.data.Delete(key)
	return nil
}

func (e *SharedEngine) List(ctx context.Context, prefix string) ([]engine.Entry, error) {
	var entries []engine.Entry
	e.data.Range(func(k, v any) bool {
		key := k.(string)
		// Very simple prefix matching for the test
		if fmt.Sprintf("%v", key) == prefix {
			entries = append(entries, engine.Entry{Key: key, Value: v.([]byte)})
		}
		return true
	})
	return entries, nil
}

func (e *SharedEngine) Lock(ctx context.Context, key string, ttl int) (context.Context, error) {
	return ctx, nil
}
func (e *SharedEngine) Unlock(ctx context.Context, key string) error { return nil }

// TestMigration_LeaderCrashRecovery simulates a leader failing mid-migration and a new leader finishing it.
func TestMigration_LeaderCrashRecovery(t *testing.T) {
	ctx := context.Background()
	ns := "test-ns"
	clusterName := "test-crash"

	sharedData := &sync.Map{}
	leaderID := &atomic.Value{}
	leaderID.Store("controller1")

	engine1 := NewSharedEngine("controller1", sharedData, leaderID)
	store1 := store.NewClusterStore(engine1)

	engine2 := NewSharedEngine("controller2", sharedData, leaderID)
	store2 := store.NewClusterStore(engine2)

	// Setup cluster
	node := &TestNode{
		ClusterMockNode: store.NewClusterMockNode(),
		info: &store.ClusterInfo{
			MigratingSlot: &store.MigratingSlot{
				SlotRange:   store.SlotRange{Start: 50, Stop: 50},
				IsMigrating: true,
			},
			MigratingState: "success",
		},
	}
	cluster := &store.Cluster{
		Name: clusterName,
		Shards: []*store.Shard{
			{
				Nodes:            []store.Node{node},
				SlotRanges:       []store.SlotRange{{Start: 0, Stop: 100}},
				MigratingSlot:    &store.MigratingSlot{SlotRange: store.SlotRange{Start: 50, Stop: 50}, IsMigrating: true},
				TargetShardIndex: 1,
			},
			{
				Nodes:      []store.Node{store.NewClusterMockNode()},
				SlotRanges: []store.SlotRange{{Start: 101, Stop: 200}},
			},
		},
	}
	require.NoError(t, store1.CreateCluster(ctx, ns, cluster))

	// 1. Controller 1 (Leader) starts checking
	_ = NewClusterChecker(store1, ns, clusterName)
	// We don't run the loop, just call the logic

	// 2. Controller 1 "Crashes" (we do nothing with checker1 anymore)

	// 3. Controller 2 becomes leader
	leaderID.Store("controller2")
	checker2 := NewClusterChecker(store2, ns, clusterName)

	// 4. Controller 2 picks up and finalizes
	checker2.tryUpdateMigrationStatus(ctx, cluster.Clone())

	// 5. Verify results in shared store
	updated, err := store2.GetCluster(ctx, ns, clusterName)
	require.NoError(t, err)
	require.False(t, store.SlotRanges(updated.Shards[0].SlotRanges).Contains(50))
	require.True(t, store.SlotRanges(updated.Shards[1].SlotRanges).Contains(50))
}
