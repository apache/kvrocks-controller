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

	sort.Strings(gotNamespaces)
	require.Equal(t, namespaces, gotNamespaces)
	for _, namespace := range namespaces {
		require.NoError(t, storage.RemoveNamespace(ctx, namespace))
	}
}

func TestStorage_Cluster(t *testing.T) {
	requireStorage(t)
}
