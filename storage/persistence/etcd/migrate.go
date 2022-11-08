package etcd

import (
	"context"
	"encoding/json"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/KvrocksLabs/kvrocks_controller/metadata"
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

func (e *Etcd) AddPendingMigrateTask(ctx context.Context, ns, cluster string, tasks []*MigrateTask) error {
	for _, task := range tasks {
		taskKey := buildMigrateTaskKey(ns, cluster, task.TaskID, task.SubID)
		taskData, err := json.Marshal(task)
		if err != nil {
			return err
		}
		if _, err := e.kv.Put(ctx, taskKey, string(taskData)); err != nil {
			return err
		}
	}
	return nil
}

func (e *Etcd) RemovePendingMigrateTask(ctx context.Context, task *MigrateTask) error {
	taskKey := buildMigrateTaskKey(task.Namespace, task.Cluster, task.TaskID, task.SubID)
	_, err := e.kv.Delete(ctx, taskKey)
	return err
}

func (e *Etcd) GetPendingMigrateTasks(ctx context.Context, ns, cluster string) ([]*MigrateTask, error) {
	prefixKey := buildMigrateTaskKeyPrefix(ns, cluster)
	resp, err := e.kv.Get(ctx, prefixKey, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	var tasks []*MigrateTask
	for _, kv := range resp.Kvs {
		if string(kv.Key) == prefixKey {
			continue
		}
		var task MigrateTask
		if err = json.Unmarshal(kv.Value, &task); err != nil {
			return nil, err
		}
		tasks = append(tasks, &task)
	}
	return tasks, nil
}

func (e *Etcd) AddMigrateTask(ctx context.Context, task *MigrateTask) error {
	taskData, err := json.Marshal(task)
	if err != nil {
		return err
	}
	_, err = e.kv.Put(ctx, buildMigratingKeyPrefix(task.Namespace, task.Cluster), string(taskData))
	return err
}

func (e *Etcd) GetMigrateTask(ctx context.Context, ns, cluster string) (*MigrateTask, error) {
	taskKey := buildMigratingKeyPrefix(ns, cluster)
	resp, err := e.kv.Get(ctx, taskKey)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, nil // nolint
	}
	var task MigrateTask
	if err := json.Unmarshal(resp.Kvs[0].Value, &task); err != nil {
		return nil, err
	}
	return &task, nil
}

func (e *Etcd) AddMigrateHistory(ctx context.Context, task *MigrateTask) error {
	taskKey := buildMigrateHistoryKey(task.Namespace, task.Cluster, task.TaskID, task.SubID)
	taskData, err := json.Marshal(task)
	if err != nil {
		return err
	}
	_, err = e.kv.Put(ctx, taskKey, string(taskData))
	if err != nil {
		return err
	}
	return nil
}

func (e *Etcd) GetMigrateHistory(ctx context.Context, ns, cluster string) ([]*MigrateTask, error) {
	prefixKey := buildMigrateHistoryPrefix(ns, cluster)
	resp, err := e.kv.Get(ctx, prefixKey, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	var tasks []*MigrateTask
	for _, kv := range resp.Kvs {
		if string(kv.Key) == prefixKey {
			continue
		}
		var task MigrateTask
		if err = json.Unmarshal(kv.Value, &task); err != nil {
			return nil, err
		}
		tasks = append(tasks, &task)
	}
	return tasks, nil
}

func (e *Etcd) IsMigrateTaskExists(ctx context.Context, ns, cluster string, taskID uint64) (bool, error) {
	taskKey := buildMigrateTaskIDPrefix(ns, cluster, taskID)
	resp, _ := e.kv.Get(ctx, taskKey, clientv3.WithPrefix())
	if len(resp.Kvs) != 0 {
		return true, nil
	}
	historyKey := buildMigrateHistoryTaskPrefix(ns, cluster, taskID)
	resp, _ = e.kv.Get(ctx, historyKey, clientv3.WithPrefix())
	if len(resp.Kvs) != 0 {
		return true, nil
	}
	doingKey := buildMigratingKeyPrefix(ns, cluster)
	resp, _ = e.kv.Get(ctx, doingKey)
	if len(resp.Kvs) != 0 {
		var task MigrateTask
		if err := json.Unmarshal(resp.Kvs[0].Value, &task); err == nil {
			if task.TaskID == taskID {
				return true, nil
			}
		} else {
			return false, err
		}
	}
	return false, nil
}

func (e *Etcd) IsMigrateHistoryExists(ctx context.Context, task *MigrateTask) (bool, error) {
	taskKey := buildMigrateHistoryKey(task.Namespace, task.Cluster, task.TaskID, task.SubID)
	resp, err := e.kv.Get(ctx, taskKey)
	if err != nil {
		return false, err
	}
	return len(resp.Kvs) != 0, nil
}
