package etcd

import (
	"context"
	"encoding/json"

	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"go.etcd.io/etcd/client/v3"
)

// MigrateTask records the metadata of task
// smallest unit of interact with etcd
type MigrateTask struct {
	TaskID      uint64               `json:"taskid"` // multi tasks allow have the same taskid
	SubID       uint64               `json:"subid"`  // different subid under the same taskid
	Namespace   string               `json:"namespace"`
	Cluster     string               `json:"cluster"`
	Source      int                  `json:"source"`        // soucre shardIdx
	Target      int                  `json:"target"`        // target shardIdx
	MigrateSlot []metadata.SlotRange `json:"migrate_slots"` // migrate slots
	SlotDoing   int                  `json:"doing_slot"`

	// statistics
	PendingTime int64 `json:"pending_time"`
	DoingTime   int64 `json:"doing_time"`
	DoneTime    int64 `json:"done_time"`

	Status int    `json:"status"` // init,penging,doing,success/failed
	Err    string `json:"error"`  // if failed, Err is not nil
}

// PushMigrateTask push migrate task to queue back
func (stor *EtcdStorage) AddMigrateTask(ns, cluster string, tasks []*MigrateTask) error {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	for _, task := range tasks {
		taskKey := NsClusterMigrateTaskKey(ns, cluster, task.TaskID, task.SubID)
		taskData, err := json.Marshal(task)
		if err != nil {
			return err
		}
		_, err = stor.cli.Put(ctx, taskKey, string(taskData))
		if err != nil {
			return err
		}
	}
	return nil
}

// PopMigrateTask pop migrate task from queue front
func (stor *EtcdStorage) RemoveMigrateTask(task *MigrateTask) error {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	taskKey := NsClusterMigrateTaskKey(task.Namespace, task.Cluster, task.TaskID, task.SubID)
	if _, err := stor.cli.Delete(ctx, taskKey); err != nil {
		return err
	}
	return nil
}

// GetMigrateTasks return migrate tasks
func (stor *EtcdStorage) GetMigrateTasks(ns, cluster string) ([]*MigrateTask, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	prefixKey := NsClusterMigrateTaskKeyPrefix(ns, cluster)
	resp, err := stor.cli.Get(ctx, prefixKey, clientv3.WithPrefix())
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

// UpdateMigrateTaskDoing update doing maigrate task info
func (stor *EtcdStorage) AddDoingMigrateTask(task *MigrateTask) error {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	taskData, err := json.Marshal(task)
	if err != nil {
		return err
	}
	_, err = stor.cli.Put(ctx, NsClusterMigrateDoingKey(task.Namespace, task.Cluster), string(taskData))
	if err != nil {
		return err
	}
	return nil
}

// GetMigrateTaskDoing return doing maigrate task info
func (stor *EtcdStorage) GetMigrateTaskDoing(ns, cluster string) (*MigrateTask, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	taskKey := NsClusterMigrateDoingKey(ns, cluster)
	resp, err := stor.cli.Get(ctx, taskKey)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	var task MigrateTask
	if err := json.Unmarshal(resp.Kvs[0].Value, &task); err != nil {
		return nil, err
	}
	return &task, nil
}

// AddMigrateTaskHistory add maigrate task to history record
func (stor *EtcdStorage) AddMigrateTaskHistory(task *MigrateTask) error {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	taskKey := NsClusterMigrateHistoryKey(task.Namespace, task.Cluster, task.TaskID, task.SubID)
	taskData, err := json.Marshal(task)
	if err != nil {
		return err
	}
	_, err = stor.cli.Put(ctx, taskKey, string(taskData))
	if err != nil {
		return err
	}
	return nil
}

// GetMigrateTaskHistory return the list of maigrate tasks of history records
func (stor *EtcdStorage) GetMigrateTaskHistory(ns, cluster string) ([]*MigrateTask, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	prefixKey := NsClusterMigrateHistoryPrefix(ns, cluster)
	resp, err := stor.cli.Get(ctx, prefixKey, clientv3.WithPrefix())
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

// HasMigrateTask return an indicator whether the cluster have the maigrate task
func (stor *EtcdStorage) HasMigrateTask(ns, cluster string, taskID uint64) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	taskKey := NsClusterMigrateTaskIDPrefix(ns, cluster, taskID)
	resp, _ := stor.cli.Get(ctx, taskKey, clientv3.WithPrefix())
	if len(resp.Kvs) != 0 {
		return true, nil
	}
	historyKey := NsClusterMigrateHistoryTaskIDPrefix(ns, cluster, taskID)
	resp, _ = stor.cli.Get(ctx, historyKey, clientv3.WithPrefix())
	if len(resp.Kvs) != 0 {
		return true, nil
	}
	doingKey := NsClusterMigrateDoingKey(ns, cluster)
	resp, _ = stor.cli.Get(ctx, doingKey)
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

// HasMigrateTaskHistory return an indicator whether the cluster have the maigrate task is history
func (stor *EtcdStorage) HasMigrateTaskHistory(task *MigrateTask) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	taskKey := NsClusterMigrateHistoryKey(task.Namespace, task.Cluster, task.TaskID, task.SubID)
	resp, err := stor.cli.Get(ctx, taskKey)
	if err != nil {
		return false, err
	}
	return len(resp.Kvs) != 0, nil
}
