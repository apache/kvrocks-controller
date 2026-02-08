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
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"

	"github.com/apache/kvrocks-controller/config"
	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/controller"
	"github.com/apache/kvrocks-controller/server/middleware"
	"github.com/apache/kvrocks-controller/store"
	"github.com/apache/kvrocks-controller/store/engine"
	"github.com/apache/kvrocks-controller/util"
)

// TestConcurrentMigrateSlot verifies that concurrent slot migrations are handled safely.
// It simulates multiple clients emitting overlapping migration tasks and asserts:
// 1. Cluster consistency (every slot belongs to exactly one shard).
// 2. Monotonic version progression.
// 3. Data integrity after migration.
// 4. Recovery to a steady state (no stuck migrations).
//
// This test requires a running Kvrocks/Redis instance on ports 7770 and 7771.
// It gracefully skips if the environment is not set up.
func TestConcurrentMigrateSlot(t *testing.T) {
	// 1. Setup Environment
	ns := "test-ns"
	clusterName := "test-concurrent-cluster"
	ctx := context.Background()
	nodeAddrs := []string{"127.0.0.1:7770", "127.0.0.1:7771"}

	// Use a mock engine for metadata storage, but real Redis for data node interaction
	clusterStore := store.NewClusterStore(engine.NewMock())
	handler := &ClusterHandler{s: clusterStore}

	// Graceful Skip: checks availability of the backend
	if err := redis.NewClient(&redis.Options{Addr: nodeAddrs[0]}).Ping(ctx).Err(); err != nil {
		t.Skipf("Skipping integration test against real redis/kvrocks: %v", err)
	}

	sourceRedisClient := redis.NewClient(&redis.Options{Addr: nodeAddrs[0]})
	targetRedisClient := redis.NewClient(&redis.Options{Addr: nodeAddrs[1]})
	defer sourceRedisClient.Close()
	defer targetRedisClient.Close()

	// 2. Initialize Cluster
	cluster, err := store.NewCluster(clusterName, nodeAddrs, 1)
	require.NoError(t, err)
	// Ensure clean state on nodes
	require.NoError(t, cluster.Reset(ctx))
	defer func() { _ = cluster.Reset(ctx) }()
	require.NoError(t, cluster.SyncToNodes(ctx))
	require.NoError(t, clusterStore.CreateCluster(ctx, ns, cluster))

	// 3. Pre-populate Data
	// Writing to slots 10-30 to verify data migration
	dataSlotStart := 10
	dataSlotEnd := 30
	testValue := "concurrent-test-value"
	for i := dataSlotStart; i <= dataSlotEnd; i++ {
		require.NoError(t, sourceRedisClient.Set(ctx, util.SlotTable[i], testValue, 0).Err())
	}

	// 4. Start Controller
	ctrl, err := controller.New(clusterStore, &config.ControllerConfig{
		FailOver: &config.FailOverConfig{
			PingIntervalSeconds: 1,
			MaxPingCount:        3,
		},
	})
	require.NoError(t, err)
	require.NoError(t, ctrl.Start(ctx))
	defer ctrl.Close()
	ctrl.WaitForReady()

	initialCluster, _ := clusterStore.GetCluster(ctx, ns, clusterName)
	initialVersion := initialCluster.Version.Load()

	// 5. Execute Concurrent Migrations
	var wg sync.WaitGroup
	routineCount := 10
	errCh := make(chan error, routineCount)

	// Launch parallel migration requests
	// Routines will overlap on some slots to test conflict handling (HTTP 409)
	for i := 0; i < routineCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			// Pattern: Overlap slots to induce race conditions
			// Routine 0, 1 -> Slot 10
			// Routine 2, 3 -> Slot 11 ...
			targetSlot := dataSlotStart + (idx / 2)
			if targetSlot > dataSlotEnd {
				targetSlot = dataSlotEnd
			}
			slotRange := store.SlotRange{Start: targetSlot, Stop: targetSlot}

			recorder := httptest.NewRecorder()
			reqCtx := GetTestContext(recorder)
			reqCtx.Set(consts.ContextKeyStore, handler.s)
			reqCtx.Params = []gin.Param{{Key: "namespace", Value: ns}, {Key: "cluster", Value: clusterName}}

			body, _ := json.Marshal(&MigrateSlotRequest{Target: 1, Slot: slotRange})
			reqCtx.Request.Body = io.NopCloser(bytes.NewBuffer(body))

			middleware.RequiredCluster(reqCtx)
			handler.MigrateSlot(reqCtx)

			// We strictly permit only OK (succeeded) or Conflict (already migrating)
			if recorder.Code != http.StatusOK && recorder.Code != http.StatusConflict {
				errCh <- fmt.Errorf("routine %d failed: code=%d body=%s", idx, recorder.Code, recorder.Body.String())
			}
		}(i)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}

	// 6. Verify Steady State
	// The cluster must eventually stabilize with no shards in a "migrating" state
	require.Eventually(t, func() bool {
		gotCluster, err := clusterStore.GetCluster(ctx, ns, clusterName)
		if err != nil {
			return false
		}
		for _, shard := range gotCluster.Shards {
			if shard.IsMigrating() {
				return false
			}
		}
		return true
	}, 30*time.Second, 500*time.Millisecond, "Cluster did not return to steady state")

	// 7. validate Invariants
	finalCluster, err := clusterStore.GetCluster(ctx, ns, clusterName)
	require.NoError(t, err)

	verifyClusterInvariants(t, finalCluster, initialVersion)
	verifyDataConsistency(t, ctx, finalCluster, sourceRedisClient, targetRedisClient, dataSlotStart, dataSlotEnd, routineCount, testValue)
}

// verifyClusterInvariants performs structural integrity checks on the cluster topology
func verifyClusterInvariants(t *testing.T, cluster *store.Cluster, initialVersion int64) {
	// Invariant: Version monotonicity
	// We expect version to increase if ANY migration succeeded.
	// If all conflicted (unlikely but possible), it might stay same, but definitely shouldn't decrease or reset.
	require.GreaterOrEqual(t, int64(cluster.Version.Load()), initialVersion, "Cluster version regressed")

	// Invariant: Slot Coverage
	// Every slot [0, len(SlotTable)-1] must belong to exactly one shard.
	slotOwners := make(map[int]int)
	for _, shard := range cluster.Shards {
		for _, r := range shard.SlotRanges {
			for s := r.Start; s <= r.Stop; s++ {
				slotOwners[s]++
			}
		}
	}

	totalSlots := len(util.SlotTable)
	for s := 0; s < totalSlots; s++ {
		count := slotOwners[s]
		if count == 0 {
			t.Errorf("Invariant failed: Slot %d is missing from all shards", s)
		} else if count > 1 {
			t.Errorf("Invariant failed: Slot %d is assigned to %d shards (overlap detected)", s, count)
		}
	}
}

// verifyDataConsistency checks that data exists on the expected owner node
func verifyDataConsistency(t *testing.T, ctx context.Context, cluster *store.Cluster, sourceClient, targetClient *redis.Client, start, end, routineCount int, expectedVal string) {
	targetShardSlots := cluster.Shards[1].SlotRanges
	processedSlots := make(map[int]bool)

	// Check the slots we attempted to migrate
	for i := 0; i < routineCount; i++ {
		targetSlot := start + (i / 2)
		if targetSlot > end {
			targetSlot = end
		}

		if processedSlots[targetSlot] {
			continue
		}
		processedSlots[targetSlot] = true

		// Determine expected owner
		isTarget := false
		for _, r := range targetShardSlots {
			if targetSlot >= r.Start && targetSlot <= r.Stop {
				isTarget = true
				break
			}
		}

		client := sourceClient
		if isTarget {
			client = targetClient
		}

		val, err := client.Get(ctx, util.SlotTable[targetSlot]).Result()
		if err != nil {
			t.Errorf("Data lost for slot %d on expected owner (isTarget=%v): %v", targetSlot, isTarget, err)
		} else if val != expectedVal {
			t.Errorf("Data corrupted for slot %d: got '%s' want '%s'", targetSlot, val, expectedVal)
		}
	}
}

func resultError(recorder *httptest.ResponseRecorder) error {
	return fmt.Errorf("code: %d, body: %s", recorder.Code, recorder.Body.String())
}
