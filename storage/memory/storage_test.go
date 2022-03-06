package memory

import (
	"testing"

	"github.com/KvrocksLabs/kvrocks-controller/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestMemStorage struct {
	*MemStorage
	shutdown chan struct{}
}

func CreateTestMemoryStorage() *TestMemStorage {
	storage := &TestMemStorage{
		MemStorage: NewMemStorage(),
		shutdown:   make(chan struct{}),
	}

	go func() {
		for {
			select {
			case <-storage.shutdown:
				return
			case <-storage.Notify():
				/* drink events */
			}
		}
	}()
	return storage
}

func (storage *TestMemStorage) Close() error {
	close(storage.shutdown)
	return storage.MemStorage.Close()
}

func TestMemStorage_CreateNamespace(t *testing.T) {
	stor := CreateTestMemoryStorage()
	ns := "test-ns"
	require.Nil(t, stor.CreateNamespace(ns))
	err := stor.CreateNamespace(ns)
	require.NotNil(t, err)
	assert.Equal(t, metadata.CodeExisted, err.(*metadata.Error).Code)
	require.Nil(t, stor.RemoveNamespace(ns))
}

func TestMemStorage_CreateCluster(t *testing.T) {
	stor := CreateTestMemoryStorage()
	ns := "test-ns"
	cluster := "test-cluster"
	require.Nil(t, stor.CreateNamespace(ns))
	// no shard
	require.NotNil(t, stor.CreateCluster(ns, cluster, nil))
	require.Nil(t, stor.CreateCluster(ns, cluster, []metadata.Shard{
		{Nodes: []metadata.NodeInfo{}},
	}))
	require.Equal(t, metadata.ErrClusterHasExisted, stor.CreateCluster(ns, cluster, nil))
	require.Nil(t, stor.RemoveCluster(ns, cluster))
	require.Nil(t, stor.RemoveNamespace(ns))
}

func TestMemStorage_CreateShard(t *testing.T) {
	stor := CreateTestMemoryStorage()
	ns := "test-ns"
	cluster := "test-cluster"
	require.Nil(t, stor.CreateNamespace(ns))
	require.Nil(t, stor.CreateCluster(ns, cluster, []metadata.Shard{
		{Nodes: []metadata.NodeInfo{}},
	}))
	err := stor.CreateShard(ns, cluster, &metadata.Shard{Nodes: []metadata.NodeInfo{}})
	require.Nil(t, err)
	require.Nil(t, stor.RemoveShard(ns, cluster, 1))
	require.Nil(t, stor.RemoveCluster(ns, cluster))
	require.Nil(t, stor.RemoveNamespace(ns))
}

func TestMemStorage_CreateNode(t *testing.T) {
	stor := CreateTestMemoryStorage()
	ns := "test-ns"
	cluster := "test-cluster"
	node := &metadata.NodeInfo{
		Role: metadata.RoleMaster,
		ID:   "test-id",
	}
	require.Nil(t, stor.CreateNamespace(ns))
	require.Nil(t, stor.CreateCluster(ns, cluster, []metadata.Shard{
		{Nodes: []metadata.NodeInfo{}},
	}))
	err := stor.CreateShard(ns, cluster, &metadata.Shard{Nodes: []metadata.NodeInfo{}})
	require.Nil(t, err)
	require.Nil(t, stor.CreateNode(ns, cluster, 0, node))
	err = stor.CreateNode(ns, cluster, 0, node)
	require.NotNil(t, err)
	assert.Equal(t, metadata.CodeExisted, err.(*metadata.Error).Code)
	require.Nil(t, stor.RemoveNode(ns, cluster, 0, node.ID))
	require.Nil(t, stor.RemoveShard(ns, cluster, 0))
	require.Nil(t, stor.RemoveCluster(ns, cluster))
	require.Nil(t, stor.RemoveNamespace(ns))
}
