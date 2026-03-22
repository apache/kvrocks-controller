//go:build integration

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
package integration

import (
	"context"
	"encoding/json"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/kvrocks-controller/config"
	"github.com/apache/kvrocks-controller/controller"
	"github.com/apache/kvrocks-controller/store"
	"github.com/apache/kvrocks-controller/store/engine"
)

const (
	testNodeID   = "07c37dfeb235213a872192d90877d0cd55635b91"
	shortLeaseMs = int64(200) // ms; tests wait 400ms for expiry
)

// sendHeartbeat sends a single CLUSTERX HEARTBEAT to h with the given params.
func sendHeartbeat(t testing.TB, h *KvrocksHandle, leaseMs int64, electionVersion uint64) error {
	t.Helper()
	ctx := context.Background()
	c := h.Client()
	defer c.Close()
	return c.Do(ctx, "CLUSTERX", "HEARTBEAT", testNodeID, leaseMs, electionVersion).Err()
}

// TestLease_Disabled verifies that in disabled mode writes always succeed even after lease expiry.
func TestLease_Disabled(t *testing.T) {
	t.Parallel()
	h := startKvrocks(t, map[string]string{"cluster-enabled": "yes"})
	initCluster(t, h, testNodeID)

	// Renew once, then wait for TTL × 2.
	require.NoError(t, sendHeartbeat(t, h, shortLeaseMs, 1))
	time.Sleep(time.Duration(shortLeaseMs*2) * time.Millisecond)

	ctx := context.Background()
	c := h.Client()
	defer c.Close()
	require.NoError(t, c.Set(ctx, "key-disabled", "v", 0).Err())
}

// TestLease_LogOnly verifies that in log-only mode writes succeed and the log contains the expiry message.
func TestLease_LogOnly(t *testing.T) {
	t.Parallel()
	h := startKvrocks(t, map[string]string{"cluster-enabled": "yes"})
	initCluster(t, h, testNodeID)
	h.ConfigSet("master-lease-mode", "log-only")

	require.NoError(t, sendHeartbeat(t, h, shortLeaseMs, 1))
	time.Sleep(time.Duration(shortLeaseMs*2) * time.Millisecond)

	ctx := context.Background()
	c := h.Client()
	defer c.Close()
	// Write must succeed in log-only mode.
	require.NoError(t, c.Set(ctx, "key-logonly", "v", 0).Err())

	// Log file must mention lease expiry.
	content, err := os.ReadFile(h.LogPath())
	require.NoError(t, err)
	re := regexp.MustCompile(`(?i)master lease expired`)
	require.True(t, re.Match(content), "log file should contain 'master lease expired'")
}

// TestLease_BlockWrite verifies that in block-write mode writes are rejected after expiry
// and succeed again after switching back to disabled.
func TestLease_BlockWrite(t *testing.T) {
	t.Parallel()
	h := startKvrocks(t, map[string]string{"cluster-enabled": "yes"})
	initCluster(t, h, testNodeID)
	h.ConfigSet("master-lease-mode", "block-write")

	require.NoError(t, sendHeartbeat(t, h, shortLeaseMs, 1))
	time.Sleep(time.Duration(shortLeaseMs*2) * time.Millisecond)

	ctx := context.Background()
	c := h.Client()
	defer c.Close()

	// Write must be rejected.
	err := c.Set(ctx, "key-block", "v", 0).Err()
	require.Error(t, err)
	require.Contains(t, err.Error(), "master lease expired")

	// Switching to disabled must re-enable writes immediately.
	h.ConfigSet("master-lease-mode", "disabled")
	require.NoError(t, c.Set(ctx, "key-block", "v", 0).Err())
}

// TestLease_ContinuousRenewal verifies that ClusterChecker keeps the lease alive
// and writes fail once the checker is stopped and the TTL expires.
func TestLease_ContinuousRenewal(t *testing.T) {
	t.Parallel()
	h := startKvrocks(t, map[string]string{"cluster-enabled": "yes"})
	initCluster(t, h, testNodeID)
	h.ConfigSet("master-lease-mode", "block-write")

	// Build a real ClusterChecker backed by a mock store containing a single-node cluster.
	mockStore := store.NewClusterStore(engine.NewMock())
	nodeAddr := h.Addr()

	// Build minimal cluster pointing at our kvrocks instance.
	clusterObj, err := store.NewCluster("test", []string{nodeAddr}, 1)
	require.NoError(t, err)

	// Override the auto-generated node ID with the one we initialised on kvrocks.
	// ClusterNode.id is unexported; inject via JSON unmarshal.
	nodeJSON, err := json.Marshal(map[string]interface{}{
		"id": testNodeID, "addr": nodeAddr, "role": "master", "password": "", "created_at": 0,
	})
	require.NoError(t, err)
	newNode := &store.ClusterNode{}
	require.NoError(t, json.Unmarshal(nodeJSON, newNode))
	clusterObj.Shards[0].Nodes[0] = newNode

	ctx := context.Background()
	require.NoError(t, mockStore.CreateCluster(ctx, "ns", clusterObj))

	checker := controller.NewClusterChecker(mockStore, "ns", "test").
		WithPingInterval(100 * time.Millisecond).
		WithLeaseConfig(&config.LeaseConfig{Enabled: true, LeaseMs: 500})
	checker.Start()

	// Writes should succeed continuously for 1 second while checker runs.
	deadline := time.Now().Add(1 * time.Second)
	c := h.Client()
	defer c.Close()
	for time.Now().Before(deadline) {
		require.NoError(t, c.Set(ctx, "key-renew", "v", 0).Err())
		time.Sleep(50 * time.Millisecond)
	}

	checker.Close()

	// After checker stops, wait for lease to expire.
	time.Sleep(600 * time.Millisecond)
	err = c.Set(ctx, "key-renew", "v", 0).Err()
	require.Error(t, err)
	require.Contains(t, err.Error(), "master lease expired")
}

// TestLease_StaleElectionVersion verifies that a HEARTBEAT with a stale election_version
// is rejected and the active lease (from a newer version) remains valid.
func TestLease_StaleElectionVersion(t *testing.T) {
	t.Parallel()
	h := startKvrocks(t, map[string]string{"cluster-enabled": "yes"})
	initCluster(t, h, testNodeID)
	h.ConfigSet("master-lease-mode", "block-write")

	// Establish version 5 with a long TTL.
	require.NoError(t, sendHeartbeat(t, h, 5000, 5))

	// Stale version 3 must be rejected.
	err := sendHeartbeat(t, h, 5000, 3)
	require.Error(t, err)
	require.Contains(t, err.Error(), "election version mismatch")

	// The valid lease from version 5 must still allow writes.
	ctx := context.Background()
	c := h.Client()
	defer c.Close()
	require.NoError(t, c.Set(ctx, "key-stale", "v", 0).Err())
}
