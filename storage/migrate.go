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

package storage

import (
	"encoding/json"
	"errors"

	"github.com/RocksLabs/kvrocks_controller/metadata"

	"golang.org/x/net/context"
)

var (
	errNilMigrateTask = errors.New("nil migrate task")
)

type MigrationTask struct {
	Namespace string `json:"namespace"`
	Cluster   string `json:"cluster"`
	TaskID    uint64 `json:"task_id"`
	Source    int    `json:"source"`
	Target    int    `json:"target"`
	Slot      int    `json:"slot"`

	PendingTime int64 `json:"pending_time"`
	StartTime   int64 `json:"start_time"`
	FinishTime  int64 `json:"finish_time"`

	Status      int    `json:"status"`
	ErrorDetail string `json:"error_detail"`
}

func (s *Storage) AddMigratePendingTasks(ctx context.Context, ns, cluster string, tasks []*MigrationTask) error {
	if len(tasks) == 0 {
		return errNilMigrateTask
	}
	for _, task := range tasks {
		taskKey := buildMigratePendingKey(ns, cluster, task.TaskID)
		taskData, err := json.Marshal(task)
		if err != nil {
			return err
		}
		if err := s.persist.Set(ctx, taskKey, taskData); err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) RemoveMigratePendingTasks(ctx context.Context, task *MigrationTask) error {
	if task == nil {
		return errNilMigrateTask
	}
	taskKey := buildMigratePendingKey(task.Namespace, task.Cluster, task.TaskID)
	return s.persist.Delete(ctx, taskKey)
}

func (s *Storage) GetMigratePendingTasks(ctx context.Context, ns, cluster string) ([]*MigrationTask, error) {
	prefixKey := buildMigrateTaskKeyPrefix(ns, cluster)
	entries, err := s.persist.List(ctx, prefixKey)
	if err != nil {
		return nil, err
	}
	tasks := make([]*MigrationTask, 0)
	for _, entry := range entries {
		var task MigrationTask
		if err = json.Unmarshal(entry.Value, &task); err != nil {
			return nil, err
		}
		tasks = append(tasks, &task)
	}
	return tasks, nil
}

func (s *Storage) AddMigratingTask(ctx context.Context, task *MigrationTask) error {
	taskData, err := json.Marshal(task)
	if err != nil {
		return err
	}
	return s.persist.Set(ctx, buildMigratingKeyPrefix(task.Namespace, task.Cluster), taskData)
}

func (s *Storage) GetMigratingTask(ctx context.Context, ns, cluster string) (*MigrationTask, error) {
	taskKey := buildMigratingKeyPrefix(ns, cluster)
	value, err := s.persist.Get(ctx, taskKey)
	if err != nil && !errors.Is(err, metadata.ErrEntryNoExists) {
		return nil, err
	}
	if len(value) == 0 {
		return nil, nil // nolint
	}
	var task MigrationTask
	if err := json.Unmarshal(value, &task); err != nil {
		return nil, err
	}
	return &task, nil
}

func (s *Storage) AddMigrateHistory(ctx context.Context, task *MigrationTask) error {
	taskKey := buildMigrateHistoryKey(task.Namespace, task.Cluster, task.TaskID)
	taskData, err := json.Marshal(task)
	if err != nil {
		return err
	}
	return s.persist.Set(ctx, taskKey, taskData)
}

func (s *Storage) GetMigrateHistory(ctx context.Context, ns, cluster string) ([]*MigrationTask, error) {
	prefixKey := buildMigrateHistoryPrefix(ns, cluster)
	entries, err := s.persist.List(ctx, prefixKey)
	if err != nil {
		return nil, err
	}
	var tasks []*MigrationTask
	for _, entry := range entries {
		var task MigrationTask
		if err = json.Unmarshal(entry.Value, &task); err != nil {
			return nil, err
		}
		tasks = append(tasks, &task)
	}
	return tasks, nil
}

func (s *Storage) IsMigrateTaskExists(ctx context.Context, ns, cluster string, taskID uint64) (bool, error) {
	taskKey := buildMigrateTaskIDPrefix(ns, cluster, taskID)
	entries, _ := s.persist.List(ctx, taskKey)
	if len(entries) != 0 {
		return true, nil
	}
	historyKey := buildMigrateHistoryTaskPrefix(ns, cluster, taskID)
	historyEntries, _ := s.persist.List(ctx, historyKey)
	if len(historyEntries) != 0 {
		return true, nil
	}
	migratingKey := buildMigratingKeyPrefix(ns, cluster)
	value, _ := s.persist.Get(ctx, migratingKey)
	if len(value) != 0 {
		var task MigrationTask
		if err := json.Unmarshal(value, &task); err != nil {
			return false, err
		}
		if task.TaskID == taskID {
			return true, nil
		}
	}
	return false, nil
}

func (s *Storage) IsMigrateHistoryExists(ctx context.Context, task *MigrationTask) (bool, error) {
	taskKey := buildMigrateHistoryKey(task.Namespace, task.Cluster, task.TaskID)
	value, err := s.persist.Get(ctx, taskKey)
	if err != nil {
		return false, err
	}
	return len(value) != 0, nil
}
