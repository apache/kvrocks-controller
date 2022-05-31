package etcd

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func GetTasks() []*MigrateTask {
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
	endpoints := []string{"127.0.0.1:2379"}
	cli, _ := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	stor, _ := NewEtcdStorage(endpoints)
	tasks := GetTasks()
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
