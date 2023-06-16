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
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/RocksLabs/kvrocks_controller/logger"
	"github.com/RocksLabs/kvrocks_controller/metadata"
	"github.com/RocksLabs/kvrocks_controller/storage"
	"github.com/RocksLabs/kvrocks_controller/util"
)

var (
	ErrMismatchMigrateSlot   = errors.New("mismatched slot")
	ErrWrongSlotNode         = errors.New("the slot isn't belong to the node")
	ErrSlotFailed            = errors.New("migrate slot fail")
	ErrSlotCompleted         = errors.New("task has been completed")
	ErrAbortedMigrateRoutine = errors.New("aborted migrate routine")
	ErrGetClusterInfo        = errors.New("get cluster info failed")
)

var (
	checkProgressInterval = 10 * time.Second
	maxClusterErrorCount  = 16

	SlotFailed  = "failed"
	SlotSuccess = "success"
)

type Migrator struct {
	storage *storage.Storage

	pendingTasks map[string][]*storage.MigrationTask

	notifyCh   chan *storage.MigrationTask
	shutdownCh chan struct{}
	quitCh     chan struct{}

	rw sync.RWMutex
}

func New(stor *storage.Storage) *Migrator {
	migrate := &Migrator{
		storage:      stor,
		pendingTasks: make(map[string][]*storage.MigrationTask),
		notifyCh:     make(chan *storage.MigrationTask, 10),
		shutdownCh:   make(chan struct{}),
		quitCh:       make(chan struct{}),
	}
	return migrate
}

func (m *Migrator) Shutdown() {
	close(m.shutdownCh)
}

func (m *Migrator) restorePendingTasks(ctx context.Context) ([]*storage.MigrationTask, error) {
	return nil, nil
}

func (m *Migrator) Load(ctx context.Context) error {
	return nil
}

func (m *Migrator) AddTask(ctx context.Context, task *storage.MigrationTask) error {
	return nil
}

func (m *Migrator) loop() {
	for {
		select {
		case <-m.notifyCh:
		case <-m.shutdownCh:
			return
		case <-m.quitCh:
			return
		}
	}
}

func (m *Migrator) startMigrating(ctx context.Context, namespace, cluster string) {
}

func (m *Migrator) sendMigrateCommand(ctx context.Context, sourceNode, targetNode *metadata.NodeInfo, slot int) error {
	redisCli, err := util.GetRedisClient(ctx, sourceNode)
	if err != nil {
		return err
	}
	return redisCli.Do(ctx, "CLUSTERX", "migrate", strconv.Itoa(slot), targetNode.ID).Err()
}

func (m *Migrator) checkMigrateStatus(ctx context.Context,
	source *metadata.NodeInfo, task *storage.MigrationTask) error {
	tries := 0
	ticker := time.NewTicker(checkProgressInterval)
	defer ticker.Stop()
	for tries < maxClusterErrorCount {
		select {
		case <-ticker.C:
			clusterInfo, err := util.ClusterInfoCmd(ctx, source)
			if err != nil {
				tries++
				logger.Get().With(
					zap.String("node", source.Addr),
					zap.Error(err),
				).Error("Failed to get cluster info")
				continue
			}
			tries = 0
			if clusterInfo.MigratingSlot != task.Slot {
				return ErrMismatchMigrateSlot
			}
			switch clusterInfo.MigratingState {
			case SlotFailed:
				return ErrSlotFailed
			case SlotSuccess:
				return nil
			}

		case <-m.shutdownCh:
			return ErrAbortedMigrateRoutine
		case <-m.quitCh:
			return ErrAbortedMigrateRoutine
		}
	}
	return ErrGetClusterInfo
}

func (m *Migrator) migrateSlot(
	ctx context.Context,
	task *storage.MigrationTask,
	source, target *metadata.NodeInfo,
	slot int) error {
	// Check if this slot is belong to the source shard
	exists, err := m.storage.HasSlot(ctx, task.Namespace, task.Cluster, task.Source, slot)
	if err != nil {
		return err
	}
	if !exists {
		return ErrWrongSlotNode
	}

	if err := m.sendMigrateCommand(ctx, source, target, slot); err != nil {
		if strings.Contains(err.Error(), ErrSlotCompleted.Error()) {
			// The migration process has been finished,
			// we can continue to migrate the next slot
			return nil
		}
		return err
	}
	if err := m.checkMigrateStatus(ctx, source, task); err != nil {
		return err
	}
	// TODO: update slot info
	return nil
}
