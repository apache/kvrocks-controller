package migrate

import (
	"fmt"
	"time"
	"testing"
	"context"

	"go.etcd.io/etcd/client/v3"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/KvrocksLabs/kvrocks-controller/metadata"
	"github.com/KvrocksLabs/kvrocks-controller/storage"
	"github.com/KvrocksLabs/kvrocks-controller/storage/base/etcd"
)

func GetCluster() *metadata.Cluster{	
	return &metadata.Cluster{
		Version: 1,
		Shards: []metadata.Shard{
			metadata.Shard{
				Nodes:[]metadata.NodeInfo {
					metadata.NodeInfo{
						ID: 		"11eceb7f355c699a367c5f3e38ec19fca7318355",
						CreatedAt:  time.Now().Unix(),
						Address: 	"127.0.0.1:6379",
						Role: 		metadata.RoleMaster,
					},
					metadata.NodeInfo{
						ID: 		"2af34116fc8b3058dff3a724be4763a2084491c8",
						CreatedAt:  time.Now().Unix(),
						Address: 	"127.0.0.1:6866",
						Role: 		metadata.RoleSlave,
					},
				},
				SlotRanges:[]metadata.SlotRange{
					metadata.SlotRange{
						Start: 0,
						Stop:  8191,
					},
				}, 
			},
			metadata.Shard{
				Nodes:[]metadata.NodeInfo {
					metadata.NodeInfo{
						ID: 		"6d536c7eb1df4112e0b51ddb69ec5a5b7959390d",
						CreatedAt:  time.Now().Unix(),
						Address: 	"127.0.0.1:6766",
						Role: 		metadata.RoleMaster,
					},
					metadata.NodeInfo{
						ID: 		"56831e9da12dda009c51ecfe67187ade0d5f0ca3",
						CreatedAt:  time.Now().Unix(),
						Address: 	"127.0.0.1:6966",
						Role: 		metadata.RoleSlave,
					},
				},
				SlotRanges:[]metadata.SlotRange {
					metadata.SlotRange{
						Start: 8192,
						Stop:  16383,
					},
				},
			},
		},
	}
}

func GetTasks() []*etcd.MigrateTask {
	task1 := &etcd.MigrateTask{
		TaskID:    uint64(1),
		SubID:     uint64(1),
		Namespace: "testNs",
		Cluster:   "testCluster",
	   	Source:    0, 
	   	Target:    1,
	   	MigrateSlot: []metadata.SlotRange {
	   		metadata.SlotRange{
	   			Start: 0,
	   			Stop:  0,
	   		},
	   	},
	}
	task2 := &etcd.MigrateTask{
		TaskID:    uint64(1),
		SubID:     uint64(2),
		Namespace: "testNs",
		Cluster:   "testCluster",
	   	Source:    0, 
	   	Target:    1,
	   	MigrateSlot: []metadata.SlotRange {
	   		metadata.SlotRange{
	   			Start: 16383,
	   			Stop:  16383,
	   		},
	   	},
	}
	task3 := &etcd.MigrateTask{
		TaskID:    uint64(1),
		SubID:     uint64(3),
		Namespace: "testNs",
		Cluster:   "testCluster",
	   	Source:    0, 
	   	Target:    1,
	   	MigrateSlot: []metadata.SlotRange {
	   		metadata.SlotRange{
	   			Start: 2,
	   			Stop:  2,
	   		},
	   		metadata.SlotRange{
	   			Start: 3,
	   			Stop:  4,
	   		},
	   	},
	}
	task4 := &etcd.MigrateTask{
		TaskID:    uint64(1),
		SubID:     uint64(4),
		Namespace: "testNs",
		Cluster:   "testCluster",
	   	Source:    1, 
	   	Target:    0,
	   	MigrateSlot: []metadata.SlotRange {
	   		metadata.SlotRange{
	   			Start: 0,
	   			Stop:  0,
	   		},
	   	},
	}
	return []*etcd.MigrateTask{task1, task2, task3, task4}
}

func TestStorage_MigrateLoad(t *testing.T) {
	cli, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	
	ctx, cancel := context.WithTimeout(context.Background(), etcd.EtcdTimeout)
	defer cancel()
	cli.Delete(ctx, "/namespace", clientv3.WithPrefix())
	cli.Delete(ctx, "/testNs", clientv3.WithPrefix())
	cli.Delete(ctx, etcd.LeaderKey, clientv3.WithPrefix())

	stor, _ :=storage.NewStorage("127.0.0.1:9131", []string{"127.0.0.1:2379"})
	cluster := GetCluster()
	clusterSlotsStr, err := cluster.ToSlotString()
	assert.Equal(t, nil, err)
	for _, addr := range []string{"127.0.0.1:6379", "127.0.0.1:6866", "127.0.0.1:6766", "127.0.0.1:6966"} {
		cli := redis.NewClient( &redis.Options{Addr: addr,} )
		cli.Do(ctx, "CLUSTERX", "setnodes", clusterSlotsStr, "0", "force").Err()
		assert.Equal(t, nil, err)
		cli.Close()
	}
	stor.CreateNamespace("testNs")
	select {
	case e := <-stor.Notify():
		assert.Equal(t, storage.EventType(storage.EventNamespace), e.Type)
	}
	stor.CreateCluster("testNs", "testCluster", cluster)
	select {
	case e := <-stor.Notify():
		assert.Equal(t, storage.EventType(storage.EventCluster), e.Type)
	}
	tasks := GetTasks()
	stor.PushMigrateTask("testNs", "testCluster", tasks)	
	mig, _ := NewMigrate(stor)
	assert.Equal(t, nil, err)
	count := 0
	for {
		time.Sleep(time.Duration(1) * time.Second)
		if count == 5 {
			break
		}
		select {
		case e := <-stor.Notify():
			count++
			assert.Equal(t, storage.Command(storage.CommandMigrateSlots), e.Command)
			cluster, _ := stor.GetClusterCopy("testNs", "testCluster")
			clusterSlotsStr, _ := cluster.ToSlotString()
			fmt.Println(clusterSlotsStr)
			for _, addr := range []string{"127.0.0.1:6379", "127.0.0.1:6866", "127.0.0.1:6766", "127.0.0.1:6966"} {
				cli := redis.NewClient( &redis.Options{Addr: addr,} )
				_, err := cli.Do(context.Background(), "CLUSTERX", "setnodes", clusterSlotsStr, "0", "force").Result()
				assert.Equal(t, nil, err)
				cli.Close()
			}
		default:
		}
	}
}

func TestStorage_Migrate(t *testing.T) {
	cli, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	ctx, cancel := context.WithTimeout(context.Background(), etcd.EtcdTimeout)
	defer cancel()
	cli.Delete(ctx, "/namespace", clientv3.WithPrefix())
	cli.Delete(ctx, "/testNs", clientv3.WithPrefix())
	cli.Delete(ctx, etcd.LeaderKey, clientv3.WithPrefix())
	stor, _ :=storage.NewStorage("127.0.0.1:9131", []string{"127.0.0.1:2379"})
	cluster := GetCluster()
	clusterSlotsStr, err := cluster.ToSlotString()
	assert.Equal(t, nil, err)
	for _, addr := range []string{"127.0.0.1:6379", "127.0.0.1:6866", "127.0.0.1:6766", "127.0.0.1:6966"} {
		cli := redis.NewClient( &redis.Options{Addr: addr,} )
		cli.Do(ctx, "CLUSTERX", "setnodes", clusterSlotsStr, "0", "force").Err()
		assert.Equal(t, nil, err)
		cli.Close()
	}
	stor.CreateNamespace("testNs")
	select {
	case e := <-stor.Notify():
		assert.Equal(t, storage.EventType(storage.EventNamespace), e.Type)
	}
	stor.CreateCluster("testNs", "testCluster", cluster)
	select {
	case e := <-stor.Notify():
		assert.Equal(t, storage.EventType(storage.EventCluster), e.Type)
	}
	mig, _ := NewMigrate(stor)
	mig.Load()
	tasks := GetTasks()
	err = mig.AddMigrateTasks(tasks)
	assert.Equal(t, nil, err)
	for {
		time.Sleep(time.Duration(1) * time.Second)
		if tasks[3].Status == TaskSuccess || tasks[3].Status == TaskFail {
			break
		}
		select {
		case e := <-stor.Notify():
			assert.Equal(t, storage.Command(storage.CommandMigrateSlots), e.Command)
			cluster, _ := stor.GetClusterCopy("testNs", "testCluster")
			clusterSlotsStr, _ := cluster.ToSlotString()
			fmt.Println(clusterSlotsStr)
			for _, addr := range []string{"127.0.0.1:6379", "127.0.0.1:6866", "127.0.0.1:6766", "127.0.0.1:6966"} {
				cli := redis.NewClient( &redis.Options{Addr: addr,} )
				_, err := cli.Do(context.Background(), "CLUSTERX", "setnodes", clusterSlotsStr, "0", "force").Result()
				assert.Equal(t, nil, err)
				cli.Close()
			}
		default:
		}
	}
	assert.Equal(t, TaskSuccess, tasks[0].Status)
	assert.Equal(t, "source no migrate slot", tasks[1].Err)
	assert.Equal(t, TaskSuccess, tasks[2].Status)
}