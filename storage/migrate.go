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

	"github.com/apache/kvrocks-controller/metadata"

	"golang.org/x/net/context"
)

var (
	errNilMigrateTask = errors.New("nil migrate task")
)

type MigrationTask struct {
	Namespace  string             `json:"namespace"`
	Cluster    string             `json:"cluster"`
	TaskID     string             `json:"task_id"`
	Source     int                `json:"source"`
	SourceNode *metadata.NodeInfo `json:"source_node"`
	Target     int                `json:"target"`
	TargetNode *metadata.NodeInfo `json:"target_node"`
	Slot       int                `json:"slot"`

	StartTime  int64 `json:"start_time"`
	FinishTime int64 `json:"finish_time"`

	Status      int    `json:"status"`
	ErrorDetail string `json:"error_detail"`
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

func (s *Storage) RemoveMigratingTask(ctx context.Context, ns, cluster string) error {
	taskKey := buildMigratingKeyPrefix(ns, cluster)
	return s.persist.Delete(ctx, taskKey)
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

func (s *Storage) IsMigrateHistoryExists(ctx context.Context, task *MigrationTask) (bool, error) {
	taskKey := buildMigrateHistoryKey(task.Namespace, task.Cluster, task.TaskID)
	value, err := s.persist.Get(ctx, taskKey)
	if err != nil {
		return false, err
	}
	return len(value) != 0, nil
}
