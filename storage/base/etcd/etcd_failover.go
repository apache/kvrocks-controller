package etcd

import (
	"context"
	"encoding/json"

	"go.etcd.io/etcd/client/v3"
	"github.com/KvrocksLabs/kvrocks-controller/metadata"
)

type FailoverTask struct {
	Namespace   string `json:"namespace"`
	Cluster     string `json:"cluster"`
   	ShardIdx    int    `json:"shardidx"` // soucre shardIdx
   	Node        metadata.NodeInfo `json:"node"`
   	Type        int    `json:"type"`     // auto(probe) or manual(devops)
   	ProbeCount  int    `json:"probecount"`

   	// statistics
   	PendingTime int64   `json:"pending_time"`
   	DoingTime   int64   `json:"doing_time"` 
   	DoneTime    int64   `json:"done_time"`

   	Status      int     `json:"status"` // init,penging,doing,success/failed
   	Err         string  `json:"error"` // if failed, Err is not nil
}

// UpdateFailoverTaskDoing update doing failover task info
func (stor *EtcdStorage) UpdateFailoverTaskDoing(task *FailoverTask) error {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	taskData, err := json.Marshal(task)
    if err != nil {
        return err
    }
    _, err = stor.cli.Put(ctx, NsClusterFailoverDoingKey(task.Namespace, task.Cluster), string(taskData))
	if err != nil {
        return err
    }
    return nil
}

// GetFailoverTaskDoing return doing failover task info
func (stor *EtcdStorage) GetFailoverTaskDoing(ns, cluster string) (*FailoverTask, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	taskKey := NsClusterFailoverDoingKey(ns, cluster)
	resp, err := stor.cli.Get(ctx, taskKey)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	var task FailoverTask
	if err := json.Unmarshal(resp.Kvs[0].Value, &task); err != nil {
        return nil, err
    }
    return &task, nil
}

// AddFailoverHistory add failover task to history record
func (stor *EtcdStorage) AddFailoverHistory(task *FailoverTask) error {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	taskKey := NsClusterFailoverHistoryKey(task.Namespace, task.Cluster, task.Node.ID, task.PendingTime)
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

// GetFailoverHistory return the list of failover tasks of history records
func (stor *EtcdStorage) GetFailoverHistory(ns, cluster string) ([]*FailoverTask, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	prefixKey := NsClusterFailoverHistoryPrefix(ns, cluster)
	resp, err := stor.cli.Get(ctx, prefixKey, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	var tasks []*FailoverTask
	for _, kv := range resp.Kvs {
		if string(kv.Key) == prefixKey {
			continue
		}
		var task FailoverTask
		if err = json.Unmarshal(kv.Value, &task); err != nil {
	        return nil, err
	    }
		tasks = append(tasks, &task)
	}
	return tasks, nil
}