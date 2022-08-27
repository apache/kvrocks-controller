package etcd

import (
	"context"
	"encoding/json"

	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"go.etcd.io/etcd/client/v3"
)

type FailOverTask struct {
	Namespace  string            `json:"namespace"`
	Cluster    string            `json:"cluster"`
	ShardIdx   int               `json:"shard_idx"`
	Node       metadata.NodeInfo `json:"node"`
	Type       int               `json:"type"`
	ProbeCount int               `json:"probe_count"`

	PendingTime int64 `json:"pending_time"`
	DoingTime   int64 `json:"doing_time"`
	DoneTime    int64 `json:"done_time"`

	Status int    `json:"status"`
	Err    string `json:"error"`
}

func (e *Etcd) UpdateDoingFailOverTask(task *FailOverTask) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	taskData, err := json.Marshal(task)
	if err != nil {
		return err
	}
	_, err = e.cli.Put(ctx, buildDoingFailOverKey(task.Namespace, task.Cluster), string(taskData))
	if err != nil {
		return err
	}
	return nil
}

func (e *Etcd) GetDoingFailOverTask(ns, cluster string) (*FailOverTask, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	taskKey := buildDoingFailOverKey(ns, cluster)
	resp, err := e.cli.Get(ctx, taskKey)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	var task FailOverTask
	if err := json.Unmarshal(resp.Kvs[0].Value, &task); err != nil {
		return nil, err
	}
	return &task, nil
}

func (e *Etcd) AddFailOverHistory(task *FailOverTask) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	taskKey := buildFailOverHistoryKey(task.Namespace, task.Cluster, task.Node.ID, task.PendingTime)
	taskData, err := json.Marshal(task)
	if err != nil {
		return err
	}
	_, err = e.cli.Put(ctx, taskKey, string(taskData))
	if err != nil {
		return err
	}
	return nil
}

func (e *Etcd) GetFailOverHistory(ns, cluster string) ([]*FailOverTask, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	prefixKey := buildFailOverHistoryPrefix(ns, cluster)
	resp, err := e.cli.Get(ctx, prefixKey, clientv3.WithPrefix())
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
