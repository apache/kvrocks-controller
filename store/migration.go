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

type MigrationTaskStatus int

const (
	MigrationTaskPending MigrationTaskStatus = iota
	MigrationTaskMigrating
	MigrationTaskSuccess
	MigrationTaskFailed
)

func (s MigrationTaskStatus) String() string {
	switch s {
	case MigrationTaskPending:
		return "pending"
	case MigrationTaskMigrating:
		return "migrating"
	case MigrationTaskSuccess:
		return "success"
	case MigrationTaskFailed:
		return "failed"
	default:
		return "unknown"
	}
}

type MigrationTask struct {
	TaskID            string              `json:"task_id"`
	SubTasks          []SlotRange         `json:"sub_tasks"`      // Pending slots
	MigratingSlot     SlotRange           `json:"migrating_slot"` // Currently migrating
	TargetShardIdx    int                 `json:"target_shard_idx"`
	SourceShardIdx    int                 `json:"source_shard_idx"`
	Status            MigrationTaskStatus `json:"status"`
	StartTime         int64               `json:"start_time"`
	FinishTime        int64               `json:"finish_time"`
	Error             string              `json:"error"`
	SlotOnly          bool                `json:"slot_only"`           // Whether to migrate only the slot definition (no data)
	PendingSlotRanges []SlotRange         `json:"pending_slot_ranges"` // Pending slots to migrate
	Retries           int                 `json:"retries"`
	MaxRetries        int                 `json:"max_retries"`
	FailurePolicy     string              `json:"failure_policy"` // "retry", "skip", "abort"
}
