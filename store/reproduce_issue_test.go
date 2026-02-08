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

package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCluster_PromoteNewMaster_SequenceZero(t *testing.T) {
	shard := NewShard()
	shard.SlotRanges = []SlotRange{{Start: 0, Stop: 1023}}

	node0 := NewClusterMockNode()
	node0.SetRole(RoleMaster)
	node0.Sequence = 100 // Master has non-zero sequence

	node1 := NewClusterMockNode()
	node1.SetRole(RoleSlave)
	node1.Sequence = 0 // Slave has zero sequence

	shard.Nodes = []Node{node0, node1}
	cluster := &Cluster{
		Shards: Shards{shard},
	}

	ctx := context.Background()

	// Try to promote node1 (sequence 0) when master has sequence 100
	// This currently fails because of the check in getNewMasterNodeIndex
	// We want this to succeed (or determine if it should).
	// Based on the task "handle sequence zero", we likely want to allow this.
	newMasterID, err := cluster.PromoteNewMaster(ctx, 0, node1.ID(), "")
	require.NoError(t, err, "PromoteNewMaster should succeed even if sequence is 0")
	require.Equal(t, node1.ID(), newMasterID)
}
