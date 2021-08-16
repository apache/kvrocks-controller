package memory

import (
	"testing"

	"github.com/KvrocksLabs/kvrocks-controller/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemStorage_CreateNamespace(t *testing.T) {
	stor := NewMemStorage()
	ns := "test-ns"
	require.Nil(t, stor.CreateNamespace(ns))
	err := stor.CreateNamespace(ns)
	require.NotNil(t, err)
	assert.Equal(t, metadata.CodeExisted, err.(*metadata.Error).Code)
	require.Nil(t, stor.RemoveNamespace(ns))
}

func TestMemStorage_CreateCluster(t *testing.T) {
	stor := NewMemStorage()
	ns := "test-ns"
	cluster := "test-cluster"
	require.Nil(t, stor.CreateNamespace(ns))
	require.Nil(t, stor.CreateCluster(ns, cluster))
	err := stor.CreateCluster(ns, cluster)
	require.NotNil(t, err)
	assert.Equal(t, metadata.CodeExisted, err.(*metadata.Error).Code)
	require.Nil(t, stor.RemoveCluster(ns, cluster))
	require.Nil(t, stor.RemoveNamespace(ns))
}

func TestMemStorage_CreateShard(t *testing.T) {
	stor := NewMemStorage()
	ns := "test-ns"
	cluster := "test-cluster"
	shard := "test-shard"
	require.Nil(t, stor.CreateNamespace(ns))
	require.Nil(t, stor.CreateCluster(ns, cluster))
	require.Nil(t, stor.CreateShard(ns, cluster, shard))
	err := stor.CreateShard(ns, cluster, shard)
	require.NotNil(t, err)
	assert.Equal(t, metadata.CodeExisted, err.(*metadata.Error).Code)
	require.Nil(t, stor.RemoveShard(ns, cluster, shard))
	require.Nil(t, stor.RemoveCluster(ns, cluster))
	require.Nil(t, stor.RemoveNamespace(ns))
}

func TestMemStorage_CreateNode(t *testing.T) {
	stor := NewMemStorage()
	ns := "test-ns"
	cluster := "test-cluster"
	shard := "test-shard"
	node := &metadata.NodeInfo{
		ID: "test-id",
	}
	require.Nil(t, stor.CreateNamespace(ns))
	require.Nil(t, stor.CreateCluster(ns, cluster))
	require.Nil(t, stor.CreateShard(ns, cluster, shard))
	require.Nil(t, stor.CreateNode(ns, cluster, shard, node))
	err := stor.CreateNode(ns, cluster, shard, node)
	require.NotNil(t, err)
	assert.Equal(t, metadata.CodeExisted, err.(*metadata.Error).Code)
	require.Nil(t, stor.RemoveNode(ns, cluster, shard, node.ID))
	require.Nil(t, stor.RemoveShard(ns, cluster, shard))
	require.Nil(t, stor.RemoveCluster(ns, cluster))
	require.Nil(t, stor.RemoveNamespace(ns))
}
