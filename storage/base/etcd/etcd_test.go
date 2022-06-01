package etcd

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestStorage_Base(t *testing.T) {
	endpoints := []string{"0.0.0.0:23790"}
	cli, _ := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	stor, _ := NewEtcdStorage(endpoints)
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	cli.Delete(ctx, "/", clientv3.WithPrefix())
	err := stor.CreateNamespace("testNs")
	assert.Equal(t, nil, err)
	err = stor.CreateCluster("testNs", "testCluster", nil)
	assert.Equal(t, "update cluster topo is nil", err.Error())
	err = stor.RemoveCluster("testNs", "testCluster")
	assert.Equal(t, nil, err)
	err = stor.RemoveNamespace("testNs")
	assert.Equal(t, nil, err)
}

func GetFailoverTasks() []*FailoverTask {
	task1 := &FailoverTask{
		Namespace:  "testNs",
		Cluster:    "testCluster",
		ShardIdx:   0,
		Type:       1,
		ProbeCount: 2,
	}
	task2 := &FailoverTask{
		Namespace:  "testNs",
		Cluster:    "testCluster",
		ShardIdx:   1,
		Type:       1,
		ProbeCount: 2,
	}
	task3 := &FailoverTask{
		Namespace:  "testNs",
		Cluster:    "testCluster",
		ShardIdx:   2,
		Type:       0,
		ProbeCount: 2,
	}
	return []*FailoverTask{task1, task2, task3}
}

func TestStorage_Failover(t *testing.T) {
	endpoints := []string{"0.0.0.0:23790"}
	cli, _ := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	stor, _ := NewEtcdStorage(endpoints)
	tasks := GetFailoverTasks()
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	cli.Delete(ctx, "/"+tasks[0].Namespace+"/"+tasks[0].Cluster, clientv3.WithPrefix())

	err := stor.UpdateFailoverTaskDoing(tasks[0])
	assert.Equal(t, nil, err)
	task, _ := stor.GetFailoverTaskDoing(tasks[0].Namespace, tasks[0].Cluster)
	assert.Equal(t, 0, task.ShardIdx)
	err = stor.AddFailoverHistory(tasks[1])
	assert.Equal(t, nil, err)
	tasks, _ = stor.GetFailoverHistory(tasks[0].Namespace, tasks[0].Cluster)
	assert.Equal(t, 1, len(tasks))
	assert.Equal(t, 1, tasks[0].ShardIdx)
}

func GetMigTasks() []*MigrateTask {
	task1 := &MigrateTask{
		TaskID:    uint64(1),
		SubID:     uint64(1),
		Namespace: "testNs",
		Cluster:   "testCluster",
		Source:    0,
		Target:    1,
		Err:       errors.New("failed").Error(),
	}
	task2 := &MigrateTask{
		TaskID:    uint64(1),
		SubID:     uint64(2),
		Namespace: "testNs",
		Cluster:   "testCluster",
		Source:    1,
		Target:    2,
	}
	task3 := &MigrateTask{
		TaskID:    uint64(2),
		SubID:     uint64(1),
		Namespace: "testNs",
		Cluster:   "testCluster",
		Source:    0,
		Target:    1,
	}
	return []*MigrateTask{task1, task2, task3}
}

func TestStorage_Migrate(t *testing.T) {
	endpoints := []string{"0.0.0.0:23790"}
	cli, _ := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	stor, _ := NewEtcdStorage(endpoints)
	tasks := GetMigTasks()
	ctx, cancel := context.WithTimeout(context.Background(), EtcdTimeout)
	defer cancel()
	cli.Delete(ctx, "/"+tasks[0].Namespace+"/"+tasks[0].Cluster, clientv3.WithPrefix())

	stor.PushMigrateTask(tasks[0].Namespace, tasks[0].Cluster, tasks)
	has, _ := stor.HasMigrateTask(tasks[0].Namespace, tasks[0].Cluster, tasks[0].TaskID)
	assert.Equal(t, true, has)
	stor.PopMigrateTask(tasks[0])
	has, _ = stor.HasMigrateTask(tasks[0].Namespace, tasks[0].Cluster, tasks[0].TaskID)
	assert.Equal(t, true, has)
	stor.PopMigrateTask(tasks[2])
	has, _ = stor.HasMigrateTask(tasks[2].Namespace, tasks[2].Cluster, tasks[2].TaskID)
	assert.Equal(t, false, has)
	stor.UpdateMigrateTaskDoing(tasks[2])
	has, _ = stor.HasMigrateTask(tasks[2].Namespace, tasks[2].Cluster, tasks[2].TaskID)
	assert.Equal(t, true, has)

	stor.AddMigrateTaskHistory(tasks[0])
	stor.AddMigrateTaskHistory(tasks[2])
	has, _ = stor.HasMigrateTask(tasks[2].Namespace, tasks[2].Cluster, tasks[2].TaskID)
	assert.Equal(t, true, has)
	ts, _ := stor.GetMigrateTaskHistory(tasks[0].Namespace, tasks[0].Cluster)
	assert.Equal(t, 2, len(ts))
	assert.Equal(t, ts[0].TaskID, tasks[0].TaskID)
	assert.Equal(t, ts[0].SubID, tasks[0].SubID)
	assert.Equal(t, ts[1].TaskID, tasks[2].TaskID)
	assert.Equal(t, ts[1].SubID, tasks[2].SubID)
}
