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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestClusterMockNode covers the reconcile-test hooks added to the mock: a settable GetClusterInfo
// reply, a record of every SyncClusterInfo force flag, and an injectable push error.
func TestClusterMockNode(t *testing.T) {
	ctx := context.Background()
	mock := NewClusterMockNode()

	// Default: a zero-value ClusterInfo when no override is set.
	info, err := mock.GetClusterInfo(ctx)
	require.NoError(t, err)
	require.NotNil(t, info)
	require.EqualValues(t, 0, info.CurrentEpoch)

	// An injected error takes precedence and is returned (simulating an unreachable node).
	mock.ClusterInfoErr = errors.New("node is down")
	_, err = mock.GetClusterInfo(ctx)
	require.ErrorIs(t, err, mock.ClusterInfoErr)
	mock.ClusterInfoErr = nil

	// The override is returned verbatim so a test can simulate a node's applied view.
	mock.MockClusterInfo = &ClusterInfo{CurrentEpoch: 5, KnownNodes: 3, SlotsOk: 16384}
	info, err = mock.GetClusterInfo(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 5, info.CurrentEpoch)
	require.EqualValues(t, 3, info.KnownNodes)
	require.EqualValues(t, 16384, info.SlotsOk)

	// SyncClusterInfo records the force flag of every call and returns the configured error.
	cluster := &Cluster{Shards: Shards{{
		Nodes:      []Node{mock},
		SlotRanges: []SlotRange{{Start: 0, Stop: 16383}},
	}}}
	cluster.Version.Store(1)
	require.NoError(t, mock.SyncClusterInfo(ctx, cluster, ForceSyncPolicy()))
	require.NoError(t, mock.SyncClusterInfo(ctx, cluster, DefaultSyncPolicy()))
	require.Equal(t, []bool{true, false}, mock.SyncForceCalls)

	mock.SyncErr = errors.New("simulated push failure")
	require.ErrorIs(t, mock.SyncClusterInfo(ctx, cluster, ForceSyncPolicy()), mock.SyncErr)
	require.Equal(t, []bool{true, false, true}, mock.SyncForceCalls)
}
