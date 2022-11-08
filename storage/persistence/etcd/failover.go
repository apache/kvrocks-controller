package etcd

import (
	"context"
	"encoding/json"

	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type FailOverTask struct {
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

func (e *Etcd) UpdateFailOverTask(ctx context.Context, task *FailOverTask) error {
	taskData, err := json.Marshal(task)
	if err != nil {
		return err
	}
	_, err = e.kv.Put(ctx, buildFailOverKey(task.Namespace, task.Cluster), string(taskData))
	return err
}

func (e *Etcd) GetFailOverTask(ctx context.Context, ns, cluster string) (*FailOverTask, error) {
	taskKey := buildFailOverKey(ns, cluster)
	resp, err := e.kv.Get(ctx, taskKey)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, nil // nolint
	}
	var task FailOverTask
	if err := json.Unmarshal(resp.Kvs[0].Value, &task); err != nil {
		return nil, err
	}
	return &task, nil
}

func (e *Etcd) AddFailOverHistory(ctx context.Context, task *FailOverTask) error {
	taskKey := buildFailOverHistoryKey(task.Namespace, task.Cluster, task.Node.ID, task.QueuedTime)
	taskData, err := json.Marshal(task)
	if err != nil {
		return err
	}
	_, err = e.kv.Put(ctx, taskKey, string(taskData))
	return err
}

func (e *Etcd) GetFailOverHistory(ctx context.Context, ns, cluster string) ([]*FailOverTask, error) {
	prefixKey := buildFailOverHistoryPrefix(ns, cluster)
	resp, err := e.kv.Get(ctx, prefixKey, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	var tasks []*FailOverTask
	for _, kv := range resp.Kvs {
		if string(kv.Key) == prefixKey {
			continue
		}
		var task FailOverTask
		if err = json.Unmarshal(kv.Value, &task); err != nil {
			return nil, err
		}
		tasks = append(tasks, &task)
	}
	return tasks, nil
}
