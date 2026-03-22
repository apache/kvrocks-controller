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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// makeClusterWithMockNodes builds a Cluster whose shard nodes are ClusterMockNodes,
// so no real Redis connections are needed.
func makeClusterWithMockNodes(masterCount, slaveCount int) *Cluster {
	cluster := &Cluster{Name: "test"}
	shard := NewShard()
	shard.SlotRanges = []SlotRange{{Start: 0, Stop: 16383}}
	shard.TargetShardIndex = -1

	for i := 0; i < masterCount; i++ {
		n := NewClusterMockNode()
		n.SetRole(RoleMaster)
		n.Sequence = 100
		shard.Nodes = append(shard.Nodes, n)
	}
	for i := 0; i < slaveCount; i++ {
		n := NewClusterMockNode()
		n.SetRole(RoleSlave)
		n.Sequence = uint64(50 + i)
		shard.Nodes = append(shard.Nodes, n)
	}
	cluster.Shards = []*Shard{shard}
	cluster.Version.Store(1)
	return cluster
}

func TestPromoteNewMaster_IncrementsElectionVersion(t *testing.T) {
	ctx := context.Background()
	cluster := makeClusterWithMockNodes(1, 2)
	require.EqualValues(t, 0, cluster.ElectionVersion.Load())

	masterID := cluster.Shards[0].GetMasterNode().ID()
	_, err := cluster.PromoteNewMaster(ctx, 0, masterID, "")
	require.NoError(t, err)
	require.EqualValues(t, 1, cluster.ElectionVersion.Load())
}

func TestPromoteNewMaster_FailedPromotion_NoVersionBump(t *testing.T) {
	ctx := context.Background()
	// Single master, no slaves — promoteNewMaster returns ErrShardNoReplica.
	cluster := makeClusterWithMockNodes(1, 0)
	require.EqualValues(t, 0, cluster.ElectionVersion.Load())

	masterID := cluster.Shards[0].GetMasterNode().ID()
	_, err := cluster.PromoteNewMaster(ctx, 0, masterID, "")
	require.Error(t, err)
	require.EqualValues(t, 0, cluster.ElectionVersion.Load(), "ElectionVersion must not change on failed promotion")
}

func TestCluster_ElectionVersion_JSONRoundTrip(t *testing.T) {
	cluster := makeClusterWithMockNodes(1, 1)
	cluster.ElectionVersion.Store(42)
	cluster.Version.Store(7)

	data, err := json.Marshal(cluster)
	require.NoError(t, err)

	// Verify the field is present in the JSON output.
	var raw map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &raw))
	require.EqualValues(t, float64(42), raw["election_version"])

	// Verify UnmarshalJSON restores the value.
	var restored Cluster
	require.NoError(t, json.Unmarshal(data, &restored))
	require.EqualValues(t, 42, restored.ElectionVersion.Load())
	require.EqualValues(t, 7, restored.Version.Load())
}
