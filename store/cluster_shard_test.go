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
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestShard_HasOverlap(t *testing.T) {
	shard := NewShard()
	slotRange := SlotRange{Start: 0, Stop: 100}
	shard.SlotRanges = append(shard.SlotRanges, slotRange)
	require.True(t, shard.HasOverlap(slotRange))
	require.True(t, shard.HasOverlap(SlotRange{Start: 50, Stop: 150}))
	require.False(t, shard.HasOverlap(SlotRange{Start: 101, Stop: 150}))
}

func TestShard_Sort(t *testing.T) {
	shard0 := NewShard()
	shard0.SlotRanges = []SlotRange{{Start: 201, Stop: 300}}
	shard1 := NewShard()
	shard1.SlotRanges = []SlotRange{{Start: 0, Stop: 400}}
	shard2 := NewShard()
	shard2.SlotRanges = []SlotRange{{Start: 101, Stop: 500}}
	shard3 := NewShard()
	shard3.SlotRanges = []SlotRange{}
	shards := Shards{shard0, shard1, shard2, shard3}
	sort.Sort(shards)
	require.EqualValues(t, 0, shards[0].SlotRanges[0].Start)
	require.EqualValues(t, 101, shards[1].SlotRanges[0].Start)
	require.EqualValues(t, 201, shards[2].SlotRanges[0].Start)
	require.EqualValues(t, 0, len(shards[3].SlotRanges))
}

func TestShard_IsServicing(t *testing.T) {
	var err error
	shard := NewShard()
	shard.TargetShardIndex = 0
	shard.MigratingSlot = &MigratingSlot{IsMigrating: false}
	require.False(t, shard.IsServicing())

	shard.TargetShardIndex = 0
	shard.MigratingSlot = nil
	require.False(t, shard.IsServicing())

	shard.TargetShardIndex = 0
	slotRange, err := NewSlotRange(1, 1)
	require.Nil(t, err)
	shard.MigratingSlot = FromSlotRange(slotRange)
	require.True(t, shard.IsServicing())

	shard.TargetShardIndex = -1
	shard.MigratingSlot = &MigratingSlot{IsMigrating: false}
	shard.SlotRanges = []SlotRange{{Start: 0, Stop: 100}}
	require.True(t, shard.IsServicing())

	shard.SlotRanges = []SlotRange{{Start: -1, Stop: -1}}
	require.False(t, shard.IsServicing())
}

func TestToSlotsString_WithFailedSlave(t *testing.T) {
	shard := NewShard()
	shard.SlotRanges = []SlotRange{{Start: 0, Stop: 100}}

	master := NewClusterNode("127.0.0.1:6379", "")
	master.SetRole(RoleMaster)

	slave := NewClusterNode("127.0.0.1:6380", "")
	slave.SetRole(RoleSlave)
	slave.SetStatus(NodeStatusFailed)

	shard.Nodes = []Node{master, slave}

	result, err := shard.ToSlotsString()
	require.NoError(t, err)
	require.Contains(t, result, "slave,fail "+master.ID())
}

func TestToSlotsString_RejectsBlankAddr(t *testing.T) {
	// An unresolved pod hostname yields a blank host (":6380"); this must not serialize into a
	// phantom node line — ToSlotsString should fail loudly instead, for either a master or a slave.
	t.Run("slave", func(t *testing.T) {
		shard := NewShard()
		shard.SlotRanges = []SlotRange{{Start: 0, Stop: 100}}
		master := NewClusterNode("127.0.0.1:6379", "")
		master.SetRole(RoleMaster)
		slave := NewClusterNode(":6380", "")
		slave.SetRole(RoleSlave)
		shard.Nodes = []Node{master, slave}

		_, err := shard.ToSlotsString()
		require.Error(t, err)
	})

	t.Run("master", func(t *testing.T) {
		shard := NewShard()
		shard.SlotRanges = []SlotRange{{Start: 0, Stop: 100}}
		master := NewClusterNode(":6379", "")
		master.SetRole(RoleMaster)
		shard.Nodes = []Node{master}

		_, err := shard.ToSlotsString()
		require.Error(t, err)
	})
}

func TestReplicaAppliedReplOffset(t *testing.T) {
	require.Equal(t, uint64(0), ReplicaAppliedReplOffset(nil))
	require.Equal(t, uint64(10), ReplicaAppliedReplOffset(&ReplicationInfo{Role: RoleMaster, MasterReplOffset: 10}))
	require.Equal(t, uint64(20), ReplicaAppliedReplOffset(&ReplicationInfo{Role: RoleSlave, MasterReplOffset: 10, SlaveReplOffset: 20}))
	require.Equal(t, uint64(10), ReplicaAppliedReplOffset(&ReplicationInfo{Role: RoleSlave, MasterReplOffset: 10}))
}

func TestShard_waitForReplicationSync(t *testing.T) {
	shard := NewShard()
	master := &ClusterMockNode{ClusterNode: NewClusterNode("127.0.0.1:6379", "")}
	master.SetRole(RoleMaster)
	master.MasterReplOffset = 1000

	slave := &ClusterMockNode{ClusterNode: NewClusterNode("127.0.0.1:6380", "")}
	slave.SetRole(RoleSlave)
	slave.SlaveOffset = 500

	ctx := context.Background()
	opts := DefaultFailoverOptions()
	opts.SyncTimeout = 30 * time.Millisecond
	err := shard.waitForReplicationSync(ctx, master, slave, opts)
	require.Error(t, err)

	slave.SlaveOffset = 1000
	err = shard.waitForReplicationSync(ctx, master, slave, opts)
	require.NoError(t, err)
}

func TestToSlotsString_WithOnlineSlave(t *testing.T) {
	shard := NewShard()
	shard.SlotRanges = []SlotRange{{Start: 0, Stop: 100}}

	master := NewClusterNode("127.0.0.1:6379", "")
	master.SetRole(RoleMaster)

	slave := NewClusterNode("127.0.0.1:6380", "")
	slave.SetRole(RoleSlave)

	shard.Nodes = []Node{master, slave}

	result, err := shard.ToSlotsString()
	require.NoError(t, err)
	require.Contains(t, result, "slave "+master.ID())
	require.NotContains(t, result, "slave,fail")
}
