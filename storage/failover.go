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

	"github.com/apache/kvrocks-controller/metadata"

	"golang.org/x/net/context"
)

type FailoverTask struct {
	Namespace  string            `json:"namespace"`
	Cluster    string            `json:"cluster"`
	ShardIdx   int               `json:"shard_idx"`
	Node       metadata.NodeInfo `json:"node"`
	Type       int               `json:"type"`
	ProbeCount int               `json:"probe_count"`

	QueuedTime int64 `json:"pending_time"`
	StartTime  int64 `json:"start_time"`
	FinishTime int64 `json:"finish_time"`

	Status int    `json:"status"`
	Err    string `json:"error"`
}

func (s *Storage) UpdateFailOverTask(ctx context.Context, task *FailoverTask) error {
	taskData, err := json.Marshal(task)
	if err != nil {
		return err
	}
	return s.persist.Set(ctx, buildFailOverKey(task.Namespace, task.Cluster), taskData)
}

func (s *Storage) GetFailOverTask(ctx context.Context, ns, cluster string) (*FailoverTask, error) {
	taskKey := buildFailOverKey(ns, cluster)
	value, err := s.persist.Get(ctx, taskKey)
	if err != nil {
		return nil, err
	}
	if len(value) == 0 {
		return nil, nil // nolint
	}
	var task FailoverTask
	if err := json.Unmarshal(value, &task); err != nil {
		return nil, err
	}
	return &task, nil
}
