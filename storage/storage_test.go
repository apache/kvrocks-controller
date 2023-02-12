package storage

import (
	"context"
	"testing"

	"github.com/KvrocksLabs/kvrocks_controller/metadata"

	"github.com/KvrocksLabs/kvrocks_controller/storage/persistence"
	"github.com/KvrocksLabs/kvrocks_controller/storage/persistence/etcd"
	"github.com/stretchr/testify/require"
)

func newTestETCD() persistence.Persistence {
	e, _ := etcd.New("test-storage", "/test/storage/leader", []string{"127.0.0.1:2379"})
	return e
}

func TestStorage_Namespace(t *testing.T) {
	persist := newTestETCD()
	storage, err := NewStorage(persist)
	require.NoError(t, err)
	ctx := context.Background()
	namespaces := []string{"test-ns1", "test-ns2", "test-ns3"}
	for _, namespace := range namespaces {
		require.NoError(t, storage.CreateNamespace(ctx, namespace))
	}

	for _, namespace := range namespaces {
		exists, err := storage.IsNamespaceExists(ctx, namespace)
		require.NoError(t, err)
		require.True(t, exists)
	}

	require.Equal(t, metadata.ErrNamespaceExisted, storage.CreateNamespace(ctx, "test-ns1"))

	gotNamespaces, err := storage.ListNamespace(ctx)
	require.NoError(t, err)

	require.Equal(t, namespaces, gotNamespaces)
	for _, namespace := range namespaces {
		require.NoError(t, storage.RemoveNamespace(ctx, namespace))
	}
}
