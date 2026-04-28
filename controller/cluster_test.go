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

	"github.com/stretchr/testify/assert"
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

	voteCalled := make(chan struct{}, 1)
	cluster := &ClusterChecker{
		clusterStore: s,
		namespace:    ns,
		clusterName:  clusterName,
		options: ClusterCheckOptions{
			pingInterval:        time.Second,
			maxFailureCount:     3,
			enableSlaveHAUpdate: true,
			voteThresholdRatio:  0.6,
		},
		failureCounts:      make(map[string]int64),
		lastProbeTime:      make(map[string]time.Time),
		failoverProposalCh: make(chan failoverProposal, 1),
		voter: voterFunc(func(_ context.Context, req VoteRequest) (bool, error) {
			voteCalled <- struct{}{}
			return true, nil // approve so PromoteNewMaster runs
		}),
		syncCh: make(chan struct{}, 1),
	}
	cluster.ctx, cluster.cancelFn = context.WithCancel(context.Background())
	cluster.StartCoordinate()
	defer cluster.Close()

	require.EqualValues(t, 1, clusterInfo.Version.Load())
	for i := int64(0); i < cluster.options.maxFailureCount-1; i++ {
		require.EqualValues(t, i+1, cluster.increaseFailureCount(0, mockNode2))
	}
	for i := int64(0); i < cluster.options.maxFailureCount; i++ {
		cluster.increaseFailureCount(0, mockNode0)
	}

	// Voter called: fast-fail if the coordinateLoop never picks up the proposal.
	select {
	case <-voteCalled:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("voter not called within timeout")
	}

	// Wait for handleProposal to complete. UpdateCluster increments Version
	// atomically AFTER PromoteNewMaster/SetRole, so Version==2 is the correct
	// synchronization point — it prevents a data race between the coordinator
	// goroutine's SetRole write and the test goroutine's IsMaster read.
	require.Eventually(t,
		func() bool { return clusterInfo.Version.Load() == 2 },
		500*time.Millisecond, 5*time.Millisecond,
		"failover did not complete")

	require.False(t, mockNode0.IsMaster())
	// mockNode2 should become the new master since its sequence is the largest
	require.True(t, mockNode2.IsMaster())
	require.EqualValues(t, 2, clusterInfo.Version.Load())

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
			pingInterval:        time.Second,
			maxFailureCount:     3,
			enableSlaveHAUpdate: true,
			voteThresholdRatio:  0.6,
		},
		failureCounts:      make(map[string]int64),
		lastProbeTime:      make(map[string]time.Time),
		failoverProposalCh: make(chan failoverProposal, 1),
		voter:              nopVoter{},
		syncCh:             make(chan struct{}, 1),
	}
	checker.ctx, checker.cancelFn = context.WithCancel(context.Background())

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

// TestPruneStaleEntries_DirectCall verifies the pruning helper itself:
// entries for absent nodes are deleted, entries for active nodes are kept.
func TestPruneStaleEntries_DirectCall(t *testing.T) {
	checker := &ClusterChecker{
		failureCounts: map[string]int64{
			"active-node":  2,
			"removed-node": 5,
		},
		lastProbeTime: map[string]time.Time{
			"active-node":  time.Now(),
			"removed-node": time.Now(),
		},
	}

	checker.pruneStaleEntries(map[string]struct{}{"active-node": {}})

	checker.failureMu.Lock()
	_, activeExists := checker.failureCounts["active-node"]
	_, removedExists := checker.failureCounts["removed-node"]
	checker.failureMu.Unlock()
	assert.True(t, activeExists, "active node failure count must be kept")
	assert.False(t, removedExists, "removed node failure count must be pruned")

	checker.lastProbeMu.Lock()
	_, activeExists = checker.lastProbeTime["active-node"]
	_, removedExists = checker.lastProbeTime["removed-node"]
	checker.lastProbeMu.Unlock()
	assert.True(t, activeExists, "active node probe time must be kept")
	assert.False(t, removedExists, "removed node probe time must be pruned")
}

// TestParallelProbeNodes_PrunesRemovedNodes verifies the integration path:
// entries accumulated for a node that has been removed from the cluster topology
// are cleaned up on the next probe round.
func TestParallelProbeNodes_PrunesRemovedNodes(t *testing.T) {
	ctx := context.Background()
	s := NewMockClusterStore()

	activeNode := store.NewClusterMockNode()
	activeNode.SetRole(store.RoleMaster)
	removedNode := store.NewClusterMockNode()
	removedNode.SetRole(store.RoleSlave)

	fullCluster := &store.Cluster{
		Name: "test",
		Shards: []*store.Shard{{
			Nodes:            []store.Node{activeNode, removedNode},
			SlotRanges:       []store.SlotRange{{Start: 0, Stop: 16383}},
			MigratingSlot:    &store.MigratingSlot{},
			TargetShardIndex: -1,
		}},
	}
	fullCluster.Version.Store(1)
	require.NoError(t, s.CreateCluster(ctx, "ns", fullCluster))

	checker := NewClusterChecker(s, "ns", "test")

	// Simulate entries that were built up while removedNode was still active.
	checker.failureMu.Lock()
	checker.failureCounts[removedNode.ID()] = 3
	checker.failureMu.Unlock()
	checker.lastProbeMu.Lock()
	checker.lastProbeTime[removedNode.ID()] = time.Now()
	checker.lastProbeMu.Unlock()

	// Probe with a topology from which removedNode has been evicted.
	reducedCluster := &store.Cluster{
		Name: "test",
		Shards: []*store.Shard{{
			Nodes:            []store.Node{activeNode},
			SlotRanges:       []store.SlotRange{{Start: 0, Stop: 16383}},
			MigratingSlot:    &store.MigratingSlot{},
			TargetShardIndex: -1,
		}},
	}
	reducedCluster.Version.Store(1)
	checker.parallelProbeNodes(ctx, reducedCluster)

	checker.failureMu.Lock()
	_, exists := checker.failureCounts[removedNode.ID()]
	checker.failureMu.Unlock()
	assert.False(t, exists, "failure count for removed node must be pruned after probe round")

	checker.lastProbeMu.Lock()
	_, exists = checker.lastProbeTime[removedNode.ID()]
	checker.lastProbeMu.Unlock()
	assert.False(t, exists, "lastProbeTime for removed node must be pruned after probe round")
}

// newTestChecker creates a ClusterChecker with voteThresholdRatio=0.6,
// maxFailureCount=5, pingInterval=3s — soft threshold = ceil(5*0.6) = 3.
func newTestChecker() *ClusterChecker {
	checker := NewClusterChecker(nil, "test-ns", "test-cluster")
	checker.options.maxFailureCount = 5
	checker.options.voteThresholdRatio = 0.6
	checker.options.pingInterval = 3 * time.Second
	return checker
}

func TestShouldVote_SufficientCountAndFresh(t *testing.T) {
	c := newTestChecker()
	c.failureCounts["nodeA"] = 5
	c.lastProbeTime["nodeA"] = time.Now()
	assert.True(t, c.ShouldVote("nodeA").Vote)
}

func TestShouldVote_ExactSoftThreshold(t *testing.T) {
	c := newTestChecker()
	c.failureCounts["nodeA"] = 3
	c.lastProbeTime["nodeA"] = time.Now()
	assert.True(t, c.ShouldVote("nodeA").Vote)
}

func TestShouldVote_CountBelowThreshold(t *testing.T) {
	c := newTestChecker()
	c.failureCounts["nodeA"] = 2
	c.lastProbeTime["nodeA"] = time.Now()
	assert.False(t, c.ShouldVote("nodeA").Vote)
}

func TestShouldVote_StaleData(t *testing.T) {
	c := newTestChecker()
	c.failureCounts["nodeA"] = 5
	c.lastProbeTime["nodeA"] = time.Now().Add(-10 * time.Second) // stale (> 2*3s)
	assert.False(t, c.ShouldVote("nodeA").Vote)
}

func TestShouldVote_NeverProbed(t *testing.T) {
	c := newTestChecker()
	assert.False(t, c.ShouldVote("unknown").Vote)
}

func TestShouldVote_AfterReset(t *testing.T) {
	c := newTestChecker()
	c.failureCounts["nodeA"] = 5
	c.lastProbeTime["nodeA"] = time.Now()
	c.resetFailureCount("nodeA")
	assert.False(t, c.ShouldVote("nodeA").Vote)
}

// TestStopCoordinate_WaitsForLoopToExit verifies Fix 1: StopCoordinate must
// block until the coordinateLoop goroutine has fully exited, not just until the
// cancel signal has been sent. Without the fix, StopCoordinate returns while
// handleProposal is still executing, creating a window where two coordinateLoops
// can run concurrently after a rapid leader-change cycle.
func TestStopCoordinate_WaitsForLoopToExit(t *testing.T) {
	gate := make(chan struct{}) // released by test to unblock the voter
	entered := make(chan struct{}) // closed when voter goroutine is active

	checker := &ClusterChecker{
		namespace:          "ns",
		clusterName:        "test",
		failoverProposalCh: make(chan failoverProposal, 1),
		syncCh:             make(chan struct{}, 1),
		failureCounts:      make(map[string]int64),
		lastProbeTime:      make(map[string]time.Time),
		voter: voterFunc(func(_ context.Context, _ VoteRequest) (bool, error) {
			close(entered) // signal: voter is now running
			<-gate         // hold until test releases
			return false, nil
		}),
	}
	checker.ctx, checker.cancelFn = context.WithCancel(context.Background())
	checker.StartCoordinate()
	defer checker.Close()

	// Deliver a proposal so coordinateLoop enters handleProposal.
	checker.failoverProposalCh <- failoverProposal{namespace: "ns", clusterName: "test"}
	<-entered // voter is now blocked inside handleProposal

	stopped := make(chan struct{})
	go func() {
		checker.StopCoordinate()
		close(stopped)
	}()

	// StopCoordinate must NOT return while the voter still holds the gate.
	select {
	case <-stopped:
		t.Fatal("StopCoordinate returned before coordinateLoop exited")
	case <-time.After(60 * time.Millisecond):
		// correct: still waiting
	}

	// Release the voter — coordinateLoop can now exit.
	close(gate)

	select {
	case <-stopped:
		// correct
	case <-time.After(500 * time.Millisecond):
		t.Fatal("StopCoordinate did not return after coordinateLoop exited")
	}
}

// TestIncreaseFailureCount_NoSpuriousProposals verifies Fix 2: proposals must
// only be sent at exact multiples of maxFailureCount (3, 6, 9 …), not on every
// tick once count exceeds the threshold (the old "|| count > max" bug).
func TestIncreaseFailureCount_NoSpuriousProposals(t *testing.T) {
	masterNode := store.NewClusterMockNode()
	masterNode.SetRole(store.RoleMaster)

	voterCh := make(chan struct{}, 10)
	checker := &ClusterChecker{
		namespace:   "ns",
		clusterName: "test",
		options: ClusterCheckOptions{
			maxFailureCount:    3,
			voteThresholdRatio: 0.6,
		},
		failureCounts:      make(map[string]int64),
		lastProbeTime:      make(map[string]time.Time),
		failoverProposalCh: make(chan failoverProposal, 1),
		syncCh:             make(chan struct{}, 1),
		voter: voterFunc(func(_ context.Context, _ VoteRequest) (bool, error) {
			voterCh <- struct{}{}
			return false, nil
		}),
	}
	checker.ctx, checker.cancelFn = context.WithCancel(context.Background())
	checker.StartCoordinate()
	defer checker.Close()

	maxF := checker.options.maxFailureCount

	// counts 1 .. maxF-1: no proposal expected
	for i := int64(0); i < maxF-1; i++ {
		checker.increaseFailureCount(0, masterNode)
	}
	time.Sleep(30 * time.Millisecond)
	assert.Empty(t, voterCh, "no proposal before threshold")

	// count = maxF: proposal expected
	checker.increaseFailureCount(0, masterNode)
	require.Eventually(t, func() bool { return len(voterCh) >= 1 },
		300*time.Millisecond, 5*time.Millisecond, "proposal at threshold")
	<-voterCh // drain

	// count = maxF+1: must NOT generate a new proposal (the old bug)
	checker.increaseFailureCount(0, masterNode)
	time.Sleep(50 * time.Millisecond)
	assert.Empty(t, voterCh, "count=maxFailureCount+1 must not trigger a proposal")

	// count = 2*maxF: next multiple → proposal expected
	for i := int64(1); i < maxF; i++ {
		checker.increaseFailureCount(0, masterNode)
	}
	require.Eventually(t, func() bool { return len(voterCh) >= 1 },
		300*time.Millisecond, 5*time.Millisecond, "proposal at 2× threshold")
}

// voterFunc lets tests inject a voter without a real VoteCoordinator.
type voterFunc func(ctx context.Context, req VoteRequest) (bool, error)

func (f voterFunc) RequestVotes(ctx context.Context, req VoteRequest) (bool, error) {
	return f(ctx, req)
}

func TestFailoverProposal_SentOnThreshold(t *testing.T) {
	s := NewMockClusterStore()
	masterNode := store.NewClusterMockNode()
	masterNode.SetRole(store.RoleMaster)
	slaveNode := store.NewClusterMockNode()
	slaveNode.SetRole(store.RoleSlave)
	slaveNode.Sequence = 10

	ctx := context.Background()
	clusterInfo := &store.Cluster{
		Name: "test-cluster",
		Shards: []*store.Shard{{
			Nodes:            []store.Node{masterNode, slaveNode},
			SlotRanges:       []store.SlotRange{{Start: 0, Stop: 16383}},
			MigratingSlot:    &store.MigratingSlot{},
			TargetShardIndex: -1,
		}},
	}
	clusterInfo.Version.Store(1)
	require.NoError(t, s.CreateCluster(ctx, "ns", clusterInfo))

	voteCalled := make(chan VoteRequest, 1)
	checker := &ClusterChecker{
		clusterStore: s,
		namespace:    "ns",
		clusterName:  "test-cluster",
		options: ClusterCheckOptions{
			pingInterval:       time.Second,
			maxFailureCount:    3,
			voteThresholdRatio: 0.6,
		},
		failureCounts:      make(map[string]int64),
		lastProbeTime:      make(map[string]time.Time),
		failoverProposalCh: make(chan failoverProposal, 1),
		voter: voterFunc(func(_ context.Context, req VoteRequest) (bool, error) {
			voteCalled <- req
			return false, nil // return false to avoid PromoteNewMaster with partial state
		}),
		syncCh: make(chan struct{}, 1),
	}
	checker.ctx, checker.cancelFn = context.WithCancel(context.Background())
	checker.StartCoordinate()
	defer checker.Close()

	for i := int64(0); i < checker.options.maxFailureCount; i++ {
		checker.increaseFailureCount(0, masterNode)
	}

	select {
	case req := <-voteCalled:
		assert.Equal(t, masterNode.ID(), req.FailedNodeID)
		assert.Equal(t, "test-cluster", req.ClusterName)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("coordinateLoop did not call voter within timeout")
	}
}
