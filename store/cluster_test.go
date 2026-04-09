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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/kvrocks-controller/consts"
)

func TestCluster_Clone(t *testing.T) {
	cluster, err := NewCluster("test", []string{"node1", "node2", "node3"}, 1)
	require.NoError(t, err)

	clusterCopy := cluster.Clone()
	require.Equal(t, cluster.Name, clusterCopy.Name)
	require.Equal(t, cluster.Shards, clusterCopy.Shards)
}

func TestCluster_FindIndexShardBySlot(t *testing.T) {
	cluster, err := NewCluster("test", []string{"node1", "node2", "node3"}, 1)
	require.NoError(t, err)

	slotRange, err := NewSlotRange(0, 0)
	require.NoError(t, err)
	shard, err := cluster.findShardIndexBySlot(slotRange)
	require.NoError(t, err)
	require.Equal(t, 0, shard)

	slotRange, err = NewSlotRange(MaxSlotID/3+1, MaxSlotID/3+1)
	require.NoError(t, err)
	shard, err = cluster.findShardIndexBySlot(slotRange)
	require.NoError(t, err)
	require.Equal(t, 1, shard)

	slotRange, err = NewSlotRange(MaxSlotID, MaxSlotID)
	require.NoError(t, err)
	shard, err = cluster.findShardIndexBySlot(slotRange)
	require.NoError(t, err)
	require.Equal(t, 2, shard)
}

func TestCluster_PromoteNewMaster(t *testing.T) {
	shard := NewShard()
	shard.SlotRanges = []SlotRange{{Start: 0, Stop: 1023}}

	node0 := NewClusterMockNode()
	node0.SetRole(RoleMaster)

	node1 := NewClusterMockNode()
	node1.SetRole(RoleSlave)
	node1.Sequence = 200

	node2 := NewClusterMockNode()
	node2.SetRole(RoleSlave)
	node2.Sequence = 100

	node3 := NewClusterMockNode()
	node3.SetRole(RoleSlave)
	node3.Sequence = 300

	shard.Nodes = []Node{node0}
	cluster := &Cluster{
		Shards: Shards{shard},
	}

	ctx := context.Background()
	_, err := cluster.PromoteNewMaster(ctx, -1, node0.ID(), "")
	require.ErrorIs(t, err, consts.ErrIndexOutOfRange)
	_, err = cluster.PromoteNewMaster(ctx, 1, node0.ID(), "")
	require.ErrorIs(t, err, consts.ErrIndexOutOfRange)
	_, err = cluster.PromoteNewMaster(ctx, 0, node0.ID(), "")
	require.ErrorIs(t, err, consts.ErrShardNoReplica)

	shard.Nodes = append(shard.Nodes, node1, node2, node3)
	_, err = cluster.PromoteNewMaster(ctx, 0, node1.ID(), "")
	require.ErrorIs(t, err, consts.ErrNodeIsNotMaster)

	newMasterID, err := cluster.PromoteNewMaster(ctx, 0, node0.ID(), "")
	require.NoError(t, err)
	require.Equal(t, node3.ID(), newMasterID)

	// test preferredNodeID
	newMasterID, err = cluster.PromoteNewMaster(ctx, 0, node3.ID(), node2.ID())
	require.NoError(t, err)
	require.Equal(t, node2.ID(), newMasterID)
}

func TestCluster_SetNodeFailedByID(t *testing.T) {
	cluster, err := NewCluster("test", []string{"node1", "node2", "node3"}, 3)
	require.NoError(t, err)
	require.Len(t, cluster.Shards, 1)

	slaveNode := cluster.Shards[0].Nodes[1]
	require.False(t, slaveNode.Failed())

	// Set failed by ID
	err = cluster.SetNodeFailedByID(slaveNode.ID(), true)
	require.NoError(t, err)
	require.True(t, slaveNode.Failed())

	// Set back to not-failed
	err = cluster.SetNodeFailedByID(slaveNode.ID(), false)
	require.NoError(t, err)
	require.False(t, slaveNode.Failed())

	// Non-existent node ID
	err = cluster.SetNodeFailedByID("nonexistent-id", true)
	require.ErrorIs(t, err, consts.ErrNotFound)
}

func TestCluster_SetNodesOffline(t *testing.T) {
	cluster, err := NewCluster("test", []string{"node1", "node2"}, 2)
	require.NoError(t, err)
	require.Len(t, cluster.Shards, 1)

	masterAddr := cluster.Shards[0].Nodes[0].Addr()
	slaveAddr := cluster.Shards[0].Nodes[1].Addr()

	// Cannot offline master
	err = cluster.SetNodesOffline([]string{masterAddr})
	require.ErrorIs(t, err, consts.ErrCannotOfflineMaster)

	// Can offline slave
	err = cluster.SetNodesOffline([]string{slaveAddr})
	require.NoError(t, err)
	require.True(t, cluster.Shards[0].Nodes[1].Failed())

	// Addr not found
	err = cluster.SetNodesOffline([]string{"nonexistent:1234"})
	require.ErrorIs(t, err, consts.ErrNotFound)

	// Atomic: if any addr is invalid, none are applied
	cluster.Shards[0].Nodes[1].SetFailed(false)
	err = cluster.SetNodesOffline([]string{slaveAddr, "nonexistent:1234"})
	require.ErrorIs(t, err, consts.ErrNotFound)
	require.False(t, cluster.Shards[0].Nodes[1].Failed()) // not modified
}

func TestCluster_SetNodesOnline(t *testing.T) {
	cluster, err := NewCluster("test", []string{"node1", "node2"}, 2)
	require.NoError(t, err)

	slaveAddr := cluster.Shards[0].Nodes[1].Addr()

	// First offline
	err = cluster.SetNodesOffline([]string{slaveAddr})
	require.NoError(t, err)
	require.True(t, cluster.Shards[0].Nodes[1].Failed())

	// Then online
	err = cluster.SetNodesOnline([]string{slaveAddr})
	require.NoError(t, err)
	require.False(t, cluster.Shards[0].Nodes[1].Failed())
}

func TestParseCluster_WithFailFlag(t *testing.T) {
	// Build a cluster nodes string with a failed slave
	clusterStr := "cfb28ef1deee4e0fa78da86abe5d24c8589b4f09 127.0.0.1:30001 master - 0 0 1 connected 0-5460\n" +
		"e44242e22c74bbe4deab41c6a9dfb68e099f2f08 127.0.0.1:30004 slave,fail cfb28ef1deee4e0fa78da86abe5d24c8589b4f09 0 0 1 connected"

	cluster, err := ParseCluster(clusterStr)
	require.NoError(t, err)
	require.Len(t, cluster.Shards, 1)
	require.Len(t, cluster.Shards[0].Nodes, 2)

	master := cluster.Shards[0].Nodes[0]
	require.True(t, master.IsMaster())
	require.False(t, master.Failed())

	slave := cluster.Shards[0].Nodes[1]
	require.False(t, slave.IsMaster())
	require.True(t, slave.Failed())
}
