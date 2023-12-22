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
package migrate

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/metadata"
	"github.com/apache/kvrocks-controller/storage"
	"github.com/apache/kvrocks-controller/util"
	"go.uber.org/zap"
)

var (
	ErrMismatchMigrateSlot   = errors.New("mismatched slot")
	ErrWrongSlotNode         = errors.New("the slot isn't belong to the node")
	ErrSlotFailed            = errors.New("migrate slot fail")
	ErrSlotCompleted         = errors.New("task has been completed")
	ErrAbortedMigrateRoutine = errors.New("aborted migrate routine")
	ErrGetClusterInfo        = errors.New("get cluster info failed")
)

const (
	SlotFailed  = "failed"
	SlotSuccess = "success"
)

const (
	TaskStatusSuccess = iota + 1
	TaskStatusFailed
)

type Migrator struct {
	migratingTasks sync.Map
	storage        *storage.Storage

	wg         sync.WaitGroup
	shutdownCh chan struct{}
}

func New(stor *storage.Storage) *Migrator {
	m := &Migrator{
		storage:    stor,
		shutdownCh: make(chan struct{}),
	}
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.loop()
	}()
	return m
}

func (m *Migrator) Shutdown() {
	close(m.shutdownCh)
	m.wg.Wait()
	m.migratingTasks.Range(func(key, value interface{}) bool {
		m.migratingTasks.Delete(key)
		return true
	})
}

func (m *Migrator) Load(ctx context.Context) error {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.loop()
	}()

	namespaces, err := m.storage.ListNamespace(ctx)
	if err != nil {
		return err
	}

	for _, namespace := range namespaces {
		clusters, err := m.storage.ListCluster(ctx, namespace)
		if err != nil {
			return err
		}
		for _, cluster := range clusters {
			task, err := m.storage.GetMigratingTask(ctx, namespace, cluster)
			if err != nil {
				return err
			}
			if task == nil {
				continue
			}
			m.migratingTasks.Store(util.BuildClusterKey(namespace, cluster), task)
		}
	}
	return nil
}

func (m *Migrator) AddTask(ctx context.Context, task *storage.MigrationTask) error {
	migratingTask, err := m.storage.GetMigratingTask(ctx, task.Namespace, task.Cluster)
	if err != nil {
		return err
	}
	if migratingTask != nil {
		return errors.New("there is a migration task running")
	}

	sourceNode, err := m.storage.GetMasterNode(ctx, task.Namespace, task.Cluster, task.Source)
	if err != nil {
		return err
	}
	targetNode, err := m.storage.GetMasterNode(ctx, task.Namespace, task.Cluster, task.Target)
	if err != nil {
		return err
	}
	// Check if this slot is belong to the source shard
	exists, err := m.storage.HasSlot(ctx, task.Namespace, task.Cluster, task.Source, task.Slot)
	if err != nil {
		return err
	}
	if !exists {
		return ErrWrongSlotNode
	}
	if err := m.sendMigrateCommand(ctx, &sourceNode, &targetNode, task.Slot); err != nil {
		if strings.Contains(err.Error(), ErrSlotCompleted.Error()) {
			// The migration process has been finished,
			// we can continue to migrate the next slot
			return nil
		}
		return err
	}
	task.SourceNode = &sourceNode
	task.TargetNode = &targetNode
	m.migratingTasks.Store(util.BuildClusterKey(task.Namespace, task.Cluster), task)
	return m.storage.AddMigratingTask(ctx, task)
}

func (m *Migrator) loop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ctx := context.Background()
			m.migratingTasks.Range(func(key, value interface{}) bool {
				task, _ := value.(*storage.MigrationTask)
				select {
				case <-m.shutdownCh:
					return false
				default:
				}
				if err := m.checkMigrateStatus(ctx, task); err != nil {
					m.abortMigratingTask(ctx, task, err)
					return true
				}
				err := m.storage.UpdateMigrateSlotInfo(ctx, task.Namespace, task.Cluster, task.Source, task.Target, []metadata.SlotRange{
					{Start: task.Slot, Stop: task.Slot},
				})
				if err != nil {
					logger.Get().Error("update migrate slot info failed", zap.Error(err))
					return true
				}
				m.finishMigratingTask(ctx, task)
				return true
			})

		case <-m.shutdownCh:
			return
		}
	}
}

func (m *Migrator) sendMigrateCommand(ctx context.Context, sourceNode, targetNode *metadata.NodeInfo, slot int) error {
	redisCli, err := util.GetRedisClient(ctx, sourceNode)
	if err != nil {
		return err
	}
	return redisCli.Do(ctx, "CLUSTERX", "migrate", strconv.Itoa(slot), targetNode.ID).Err()
}

func (m *Migrator) checkMigrateStatus(ctx context.Context, task *storage.MigrationTask) error {
	clusterInfo, err := util.ClusterInfoCmd(ctx, task.SourceNode)
	if err != nil {
		return ErrGetClusterInfo
	}
	if clusterInfo.MigratingSlot != task.Slot {
		return ErrMismatchMigrateSlot
	}
	switch clusterInfo.MigratingState {
	case SlotFailed:
		return ErrSlotFailed
	case SlotSuccess:
		return nil
	default:
		return fmt.Errorf("unknown migrate state: %s", clusterInfo.MigratingState)
	}
}

func (m *Migrator) removeMigratingTask(ctx context.Context, task *storage.MigrationTask) error {
	task.FinishTime = time.Now().Unix()
	if err := m.storage.RemoveMigratingTask(ctx, task.Namespace, task.Cluster); err != nil {
		return err
	}
	m.migratingTasks.Delete(util.BuildClusterKey(task.Namespace, task.Cluster))
	return nil
}

func (m *Migrator) abortMigratingTask(ctx context.Context, task *storage.MigrationTask, err error) {
	task.Status = TaskStatusFailed
	task.ErrorDetail = err.Error()
	task.FinishTime = time.Now().Unix()
	_ = m.removeMigratingTask(ctx, task)
	logger.Get().With(
		zap.Error(err),
		zap.Any("task", task),
	).Error("Aborted the migrate task")
}

func (m *Migrator) finishMigratingTask(ctx context.Context, task *storage.MigrationTask) {
	task.Status = TaskStatusSuccess
	task.FinishTime = time.Now().Unix()
	_ = m.removeMigratingTask(ctx, task)
	logger.Get().With(
		zap.Any("task", task),
	).Info("Success to migrate the slot")
}
