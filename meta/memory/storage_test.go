package memory

import (
	"testing"

	"github.com/KvrocksLabs/kvrocks-controller/meta"
	"github.com/stretchr/testify/require"
)

func TestMemStorage_CreateNamespace(t *testing.T) {
	stor := NewMemStorage()
	ns := "test-ns"
	require.Nil(t, stor.CreateNamespace(ns))
	require.Equal(t, meta.ErrNamespaceExisted, stor.CreateNamespace(ns))
	require.Nil(t, stor.RemoveNamespace(ns))
}

func TestMemStorage_CreateCluster(t *testing.T) {
	stor := NewMemStorage()
	ns := "test-ns"
	cluster := "test-cluster"
	require.Nil(t, stor.CreateNamespace(ns))
	require.Nil(t, stor.CreateCluster(ns, cluster))
	require.Equal(t, meta.ErrClusterExisted, stor.CreateCluster(ns, cluster))
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
	require.Equal(t, meta.ErrShardExisted, stor.CreateShard(ns, cluster, shard))
	require.Nil(t, stor.RemoveShard(ns, cluster, shard))
	require.Nil(t, stor.RemoveCluster(ns, cluster))
	require.Nil(t, stor.RemoveNamespace(ns))
}

func TestMemStorage_CreateNode(t *testing.T) {
	stor := NewMemStorage()
	ns := "test-ns"
	cluster := "test-cluster"
	shard := "test-shard"
	node := &meta.NodeInfo{
		ID: "test-id",
	}
	require.Nil(t, stor.CreateNamespace(ns))
	require.Nil(t, stor.CreateCluster(ns, cluster))
	require.Nil(t, stor.CreateShard(ns, cluster, shard))
	require.Nil(t, stor.CreateNode(ns, cluster, shard, node))
	require.Equal(t, meta.ErrNodeExisted, stor.CreateNode(ns, cluster, shard, node))
	require.Nil(t, stor.RemoveNode(ns, cluster, shard, node.ID))
	require.Nil(t, stor.RemoveShard(ns, cluster, shard))
	require.Nil(t, stor.RemoveCluster(ns, cluster))
	require.Nil(t, stor.RemoveNamespace(ns))
}
