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

package metadata

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCluster_CheckOverlap(t *testing.T) {
	cluster := &Cluster{
		Shards: []Shard{
			{SlotRanges: []SlotRange{{Start: 3, Stop: 5000}}},
			{SlotRanges: []SlotRange{{Start: 5001, Stop: 10000}}},
			{SlotRanges: []SlotRange{{Start: 10001, Stop: 16383}}},
		},
	}
	require.NoError(t, cluster.CheckOverlap(&SlotRange{Start: 1, Stop: 2}))
	require.Error(t, cluster.CheckOverlap(&SlotRange{Start: 1, Stop: 3}))
}

func TestCluster_ToSlotString(t *testing.T) {
	clusterStr := "kvrockskvrockskvrockskvrockskvrocksnode1 127.0.0.1:30001@40001 myself,master - 1676818671568 1676818671569 1 connected 0-5460\n" +
		"kvrockskvrockskvrockskvrockskvrocksnode2 127.0.0.1:30002@40002 slave kvrockskvrockskvrockskvrockskvrocksnode1 1676818671568 1676818671569 1 connected\n" +
		"kvrockskvrockskvrockskvrockskvrocksnode3 127.0.0.1:30003@40003 master - 1676818671568 1676818671569 1 connected 5461-10992\n" +
		"kvrockskvrockskvrockskvrockskvrocksnode5 127.0.0.1:30005@40005 master - 1676818671568 1676818671569 1 connected 10993-16383\n" +
		"kvrockskvrockskvrockskvrockskvrocksnode6 127.0.0.1:30006@40006 slave kvrockskvrockskvrockskvrockskvrocksnode5 1676818671568 1676818671569 1 connected\n" +
		"kvrockskvrockskvrockskvrockskvrocksnode4 127.0.0.1:30004@40004 slave kvrockskvrockskvrockskvrockskvrocksnode3 1676818671568 1676818671569 1 connected"
	parsedCluster, err := ParseCluster(clusterStr)
	require.NoError(t, err)
	require.Equal(t, int64(1), parsedCluster.Version)

	require.Equal(t, 3, len(parsedCluster.Shards))

	require.Equal(t, []SlotRange{{Start: 0, Stop: 5460}}, parsedCluster.Shards[0].SlotRanges)
	require.Equal(t, "kvrockskvrockskvrockskvrockskvrocksnode1", parsedCluster.Shards[0].Nodes[0].ID)
	require.Equal(t, 2, len(parsedCluster.Shards[0].Nodes))
	require.Equal(t, RoleMaster, parsedCluster.Shards[0].Nodes[0].Role)
	require.Equal(t, RoleSlave, parsedCluster.Shards[0].Nodes[1].Role)

	require.Equal(t, []SlotRange{{Start: 5461, Stop: 10992}}, parsedCluster.Shards[1].SlotRanges)
	require.Equal(t, "kvrockskvrockskvrockskvrockskvrocksnode3", parsedCluster.Shards[1].Nodes[0].ID)
	require.Equal(t, 2, len(parsedCluster.Shards[1].Nodes))
	require.Equal(t, RoleMaster, parsedCluster.Shards[1].Nodes[0].Role)
	require.Equal(t, RoleSlave, parsedCluster.Shards[1].Nodes[1].Role)

	require.Equal(t, []SlotRange{{Start: 10993, Stop: 16383}}, parsedCluster.Shards[2].SlotRanges)
	require.Equal(t, "kvrockskvrockskvrockskvrockskvrocksnode5", parsedCluster.Shards[2].Nodes[0].ID)
	require.Equal(t, 2, len(parsedCluster.Shards[2].Nodes))
	require.Equal(t, RoleMaster, parsedCluster.Shards[2].Nodes[0].Role)
	require.Equal(t, RoleSlave, parsedCluster.Shards[2].Nodes[1].Role)

	expectedSlotString := "kvrockskvrockskvrockskvrockskvrocksnode1 127.0.0.1 30001 master - 0-5460\n" +
		"kvrockskvrockskvrockskvrockskvrocksnode2 127.0.0.1 30002 slave kvrockskvrockskvrockskvrockskvrocksnode1\n" +
		"kvrockskvrockskvrockskvrockskvrocksnode3 127.0.0.1 30003 master - 5461-10992\n" +
		"kvrockskvrockskvrockskvrockskvrocksnode4 127.0.0.1 30004 slave kvrockskvrockskvrockskvrockskvrocksnode3\n" +
		"kvrockskvrockskvrockskvrockskvrocksnode5 127.0.0.1 30005 master - 10993-16383\n" +
		"kvrockskvrockskvrockskvrockskvrocksnode6 127.0.0.1 30006 slave kvrockskvrockskvrockskvrockskvrocksnode5\n"
	gotSlotString, err := parsedCluster.ToSlotString()
	require.NoError(t, err)
	require.Equal(t, expectedSlotString, gotSlotString)
}
