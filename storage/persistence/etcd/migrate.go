package etcd

import (
	"context"
	"encoding/json"

	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type MigrateTask struct {
	Namespace   string               `json:"namespace"`
	Cluster     string               `json:"cluster"`
	TaskID      uint64               `json:"task_id"`
	SubID       uint64               `json:"sub_id"`
	Source      int                  `json:"source"`
	Target      int                  `json:"target"`
	MigrateSlot []metadata.SlotRange `json:"migrate_slots"`
	SlotDoing   int                  `json:"doing_slot"`

	PendingTime int64 `json:"pending_time"`
	DoingTime   int64 `json:"doing_time"`
	DoneTime    int64 `json:"done_time"`

	Status int    `json:"status"`
	Err    string `json:"error"`
}

func (e *Etcd) AddMigrateTask(ns, cluster string, tasks []*MigrateTask) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	for _, task := range tasks {
		taskKey := buildMigrateTaskKey(ns, cluster, task.TaskID, task.SubID)
		taskData, err := json.Marshal(task)
		if err != nil {
			return err
		}
		_, err = e.kv.Put(ctx, taskKey, string(taskData))
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Etcd) RemoveMigrateTask(task *MigrateTask) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	taskKey := buildMigrateTaskKey(task.Namespace, task.Cluster, task.TaskID, task.SubID)
	if _, err := e.kv.Delete(ctx, taskKey); err != nil {
		return err
	}
	return nil
}

func (e *Etcd) GetMigrateTasks(ns, cluster string) ([]*MigrateTask, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
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

func (e *Etcd) AddDoingMigrateTask(task *MigrateTask) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	taskData, err := json.Marshal(task)
	if err != nil {
		return err
	}
	_, err = e.kv.Put(ctx, buildMigratingKeyPrefix(task.Namespace, task.Cluster), string(taskData))
	if err != nil {
		return err
	}
	return nil
}

func (e *Etcd) GetDoingMigrateTask(ns, cluster string) (*MigrateTask, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
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

func (e *Etcd) AddHistoryMigrateTask(task *MigrateTask) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
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

func (e *Etcd) GetHistoryMigrateTask(ns, cluster string) ([]*MigrateTask, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
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

func (e *Etcd) IsMigrateTaskExists(ns, cluster string, taskID uint64) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
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

func (e *Etcd) IsHistoryMigrateTaskExists(task *MigrateTask) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	taskKey := buildMigrateHistoryKey(task.Namespace, task.Cluster, task.TaskID, task.SubID)
	resp, err := e.kv.Get(ctx, taskKey)
	if err != nil {
		return false, err
	}
	return len(resp.Kvs) != 0, nil
}
