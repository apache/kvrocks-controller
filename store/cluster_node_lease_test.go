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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/alicebob/miniredis/v2/server"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
)

// newNodeOnMiniRedis creates a ClusterNode whose redis client points at a fresh miniredis instance.
// The miniredis instance is closed automatically by RunT when the test ends.
func newNodeOnMiniRedis(t *testing.T) (*ClusterNode, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)

	node := &ClusterNode{
		id:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", // 40 chars
		addr: mr.Addr(),
		role: RoleMaster,
	}
	// Override the global clients map so this node uses the miniredis address.
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	clients.Store(node.id, client)
	t.Cleanup(func() { clients.Delete(node.id) })
	return node, mr
}

func TestSetLeaseParams_Stored(t *testing.T) {
	node, mr := newNodeOnMiniRedis(t)

	var mu sync.Mutex
	var capturedArgs []string
	mr.Server().Register("CLUSTERX", func(c *server.Peer, cmd string, args []string) {
		mu.Lock()
		capturedArgs = append([]string{cmd}, args...)
		mu.Unlock()
		c.WriteBulk("sequence:42\r\nrole:master\r\n")
	})

	params := LeaseParams{
		Enabled:         true,
		MasterNodeID:    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		LeaseMs:         500,
		ElectionVersion: 7,
	}
	node.SetLeaseParams(params)

	ctx := context.Background()
	info, err := node.GetClusterNodeInfo(ctx)
	require.NoError(t, err)

	// Verify the stored params were used as HEARTBEAT arguments.
	mu.Lock()
	args := capturedArgs
	mu.Unlock()
	require.Equal(t, []string{"CLUSTERX", "HEARTBEAT",
		params.MasterNodeID,
		fmt.Sprintf("%d", params.LeaseMs),
		fmt.Sprintf("%d", params.ElectionVersion),
	}, args)

	// Verify the response was parsed correctly.
	require.EqualValues(t, 42, info.Sequence)
	require.Equal(t, "master", info.Role)
}

func TestGetClusterNodeInfo_LeaseDisabled(t *testing.T) {
	node, mr := newNodeOnMiniRedis(t)

	var heartbeatCalled atomic.Bool
	mr.Server().Register("CLUSTERX", func(c *server.Peer, cmd string, args []string) {
		heartbeatCalled.Store(true)
		c.WriteBulk("sequence:0\r\nrole:master\r\n")
	})

	// leaseParams.Enabled is false (zero value) — should use INFO, not HEARTBEAT.
	ctx := context.Background()
	// INFO returns an empty string by default in miniredis; that's fine — we only
	// care that HEARTBEAT was NOT called.
	_, _ = node.GetClusterNodeInfo(ctx)

	require.False(t, heartbeatCalled.Load(), "CLUSTERX HEARTBEAT must not be called when lease is disabled")
}

func TestGetClusterNodeInfo_LeaseEnabled_SendsHeartbeat(t *testing.T) {
	node, mr := newNodeOnMiniRedis(t)

	mr.Server().Register("CLUSTERX", func(c *server.Peer, cmd string, args []string) {
		c.WriteBulk("sequence:42\r\nrole:master\r\n")
	})

	node.SetLeaseParams(LeaseParams{
		Enabled:         true,
		MasterNodeID:    "cccccccccccccccccccccccccccccccccccccccc",
		LeaseMs:         300,
		ElectionVersion: 3,
	})

	ctx := context.Background()
	info, err := node.GetClusterNodeInfo(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 42, info.Sequence)
	require.Equal(t, "master", info.Role)
}

func TestGetClusterNodeInfo_HeartbeatError(t *testing.T) {
	node, mr := newNodeOnMiniRedis(t)

	mr.Server().Register("CLUSTERX", func(c *server.Peer, cmd string, args []string) {
		c.WriteError("ERR lease error from server")
	})

	node.SetLeaseParams(LeaseParams{
		Enabled:         true,
		MasterNodeID:    "dddddddddddddddddddddddddddddddddddddddd",
		LeaseMs:         200,
		ElectionVersion: 1,
	})

	ctx := context.Background()
	_, err := node.GetClusterNodeInfo(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "lease error from server")
}
