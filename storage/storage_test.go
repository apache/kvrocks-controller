package storage

import (
	"context"
	"sort"
	"testing"

	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/storage/persistence/etcd"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"github.com/stretchr/testify/require"
)

var storage *Storage

func requireStorage(t *testing.T) {
	if storage != nil {
		return
	}
	e, err := etcd.New("test-storage", "/test/storage/leader", []string{"127.0.0.1:2379"})
	require.NoError(t, err)
	storage, err = NewStorage(e)
	require.NoError(t, err)
}

func TestStorage_Namespace(t *testing.T) {
	requireStorage(t)

	ctx := context.Background()
	namespaces := []string{util.RandString(40), util.RandString(40), util.RandString(40)}
	sort.Strings(namespaces)
	for _, namespace := range namespaces {
		require.NoError(t, storage.CreateNamespace(ctx, namespace))
	}

	for _, namespace := range namespaces {
		exists, err := storage.IsNamespaceExists(ctx, namespace)
		require.NoError(t, err)
		require.True(t, exists)
	}

	require.Equal(t, metadata.ErrNamespaceExisted, storage.CreateNamespace(ctx, namespaces[0]))

	gotNamespaces, err := storage.ListNamespace(ctx)
	require.NoError(t, err)

	for _, namespace := range namespaces {
		require.Contains(t, gotNamespaces, namespace)
		require.NoError(t, storage.RemoveNamespace(ctx, namespace))
	}
}

func TestStorage_Cluster(t *testing.T) {
	requireStorage(t)
	ns := util.RandString(10)
	clusterName := util.RandString(10)
	ctx := context.Background()

	newClusterInfo := &metadata.Cluster{
		Name: clusterName,
		Shards: []metadata.Shard{
			{
				Nodes: []metadata.NodeInfo{
					{ID: util.RandString(40), Address: "1.1.1.1:6379", Role: metadata.RoleMaster},
				},
				SlotRanges: []metadata.SlotRange{
					{Start: 0, Stop: 5000},
				},
			},
		},
	}
	err := storage.CreateCluster(ctx, ns, newClusterInfo)

	require.NoError(t, err)
	exists, err := storage.IsClusterExists(ctx, ns, clusterName)
	require.NoError(t, err)
	require.True(t, exists)
	clusterInfo, err := storage.GetClusterInfo(ctx, ns, clusterName)
	require.Equal(t, newClusterInfo, clusterInfo)
	require.NoError(t, err)
	require.NoError(t, storage.RemoveCluster(ctx, ns, clusterName))
	exists, err = storage.IsClusterExists(ctx, ns, clusterName)
	require.NoError(t, err)
	require.False(t, exists)
}
