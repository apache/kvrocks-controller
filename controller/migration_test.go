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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/kvrocks-controller/store"
	"github.com/apache/kvrocks-controller/store/engine"
)

func TestMigrationQueue(t *testing.T) {
	ns := "test-ns"
	clusterName := "test-cluster"
	s := store.NewClusterStore(engine.NewMock())
	ctx := context.Background()

	cluster, err := store.NewCluster(clusterName, []string{"127.0.0.1:1111", "127.0.0.1:2222"}, 1)
	require.NoError(t, err)
	require.NoError(t, s.CreateNamespace(ctx, ns))
	require.NoError(t, s.CreateCluster(ctx, ns, cluster))

	checker := NewClusterChecker(s, ns, clusterName)

	t.Run("Queue and Process Multiple Ranges", func(t *testing.T) {
		slots := []store.SlotRange{{Start: 10, Stop: 12}, {Start: 20, Stop: 22}}
		// Use slotOnly=true for easier unit testing (no node calls)
		err := cluster.MigrateSlots(ctx, slots, 1, true, "append", "retry", 3)
		require.NoError(t, err)
		require.NoError(t, s.UpdateCluster(ctx, ns, cluster))

		// First loop: Start task and immediately process first range
		checker.processMigrationQueue(ctx, cluster)
		require.Equal(t, store.MigrationTaskMigrating, cluster.MigrationTasks[0].Status)
		require.Equal(t, store.SlotRange{Start: 10, Stop: 12}, cluster.MigrationTasks[0].MigratingSlot)
		require.Len(t, cluster.MigrationTasks[0].PendingSlotRanges, 1)

		// With slotOnly=true, the slot is moved immediately in MigrateSlot
		// So we don't expect MigratingSlot to be set on the shard, and SlotRanges should be updated.
		require.Nil(t, cluster.Shards[0].MigratingSlot)

		// Second loop: Process second range
		checker.processMigrationQueue(ctx, cluster)
		require.Equal(t, store.SlotRange{Start: 20, Stop: 22}, cluster.MigrationTasks[0].MigratingSlot)
		require.Len(t, cluster.MigrationTasks[0].PendingSlotRanges, 0)

		// Third loop: Task success and removed
		checker.processMigrationQueue(ctx, cluster)
		require.Len(t, cluster.MigrationTasks, 0)
	})

	t.Run("Failure with Abort", func(t *testing.T) {
		cluster.MigrationTasks = nil
		slots := []store.SlotRange{{Start: 30, Stop: 32}}
		// Migrate to non-existent shard to trigger index out of range error in MigrateSlot
		err := cluster.MigrateSlots(ctx, slots, 99, true, "append", "abort", 1)
		require.NoError(t, err)
		require.NoError(t, s.UpdateCluster(ctx, ns, cluster))

		checker.processMigrationQueue(ctx, cluster) // Pending -> Migrating and starts range (fails because targetShardIdx 99 is invalid)
		require.Equal(t, 1, cluster.MigrationTasks[0].Retries)

		checker.processMigrationQueue(ctx, cluster) // Attempt 2 (fail -> abort)

		require.Equal(t, store.MigrationTaskFailed, cluster.MigrationTasks[0].Status)
	})
}
