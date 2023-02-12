package storage

import (
	"encoding/json"
	"errors"

	"github.com/KvrocksLabs/kvrocks_controller/metadata"

	"golang.org/x/net/context"
)

var (
	errNilMigrateTask = errors.New("nil migrate task")
)

type MigrateTask struct {
	Namespace     string               `json:"namespace"`
	Cluster       string               `json:"cluster"`
	TaskID        uint64               `json:"task_id"`
	SubID         uint64               `json:"sub_id"`
	Source        int                  `json:"source"`
	Target        int                  `json:"target"`
	PlanSlots     []metadata.SlotRange `json:"plan_slots"`
	MigratingSlot int                  `json:"migrating_slot"`

	PendingTime int64 `json:"pending_time"`
	StartTime   int64 `json:"start_time"`
	FinishTime  int64 `json:"finish_time"`

	Status      int    `json:"status"`
	ErrorDetail string `json:"error_detail"`
}

func (s *Storage) AddPendingMigrateTask(ctx context.Context, ns, cluster string, tasks []*MigrateTask) error {
	if len(tasks) == 0 {
		return errNilMigrateTask
	}
	for _, task := range tasks {
		taskKey := buildMigrateTaskKey(ns, cluster, task.TaskID, task.SubID)
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

func (s *Storage) RemovePendingMigrateTask(ctx context.Context, task *MigrateTask) error {
	if task == nil {
		return errNilMigrateTask
	}
	taskKey := buildMigrateTaskKey(task.Namespace, task.Cluster, task.TaskID, task.SubID)
	return s.persist.Delete(ctx, taskKey)
}

func (s *Storage) GetPendingMigrateTasks(ctx context.Context, ns, cluster string) ([]*MigrateTask, error) {
	prefixKey := buildMigrateTaskKeyPrefix(ns, cluster)
	entries, err := s.persist.List(ctx, prefixKey)
	if err != nil {
		return nil, err
	}
	tasks := make([]*MigrateTask, 0)
	for _, entry := range entries {
		var task MigrateTask
		if err = json.Unmarshal(entry.Value, &task); err != nil {
			return nil, err
		}
		tasks = append(tasks, &task)
	}
	return tasks, nil
}

func (s *Storage) AddMigrateTask(ctx context.Context, task *MigrateTask) error {
	taskData, err := json.Marshal(task)
	if err != nil {
		return err
	}
	return s.persist.Set(ctx, buildMigratingKeyPrefix(task.Namespace, task.Cluster), taskData)
}

func (s *Storage) GetMigrateTask(ctx context.Context, ns, cluster string) (*MigrateTask, error) {
	taskKey := buildMigratingKeyPrefix(ns, cluster)
	value, err := s.persist.Get(ctx, taskKey)
	if err != nil {
		return nil, err
	}
	if len(value) == 0 {
		return nil, nil // nolint
	}
	var task MigrateTask
	if err := json.Unmarshal(value, &task); err != nil {
		return nil, err
	}
	return &task, nil
}

func (s *Storage) AddMigrateHistory(ctx context.Context, task *MigrateTask) error {
	taskKey := buildMigrateHistoryKey(task.Namespace, task.Cluster, task.TaskID, task.SubID)
	taskData, err := json.Marshal(task)
	if err != nil {
		return err
	}
	return s.persist.Set(ctx, taskKey, taskData)
}

func (s *Storage) GetMigrateHistory(ctx context.Context, ns, cluster string) ([]*MigrateTask, error) {
	prefixKey := buildMigrateHistoryPrefix(ns, cluster)
	entries, err := s.persist.List(ctx, prefixKey)
	if err != nil {
		return nil, err
	}
	var tasks []*MigrateTask
	for _, entry := range entries {
		var task MigrateTask
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
		var task MigrateTask
		if err := json.Unmarshal(value, &task); err != nil {
			return false, err
		}
		if task.TaskID == taskID {
			return true, nil
		}
	}
	return false, nil
}

func (s *Storage) IsMigrateHistoryExists(ctx context.Context, task *MigrateTask) (bool, error) {
	taskKey := buildMigrateHistoryKey(task.Namespace, task.Cluster, task.TaskID, task.SubID)
	value, err := s.persist.Get(ctx, taskKey)
	if err != nil {
		return false, err
	}
	return len(value) != 0, nil
}
