package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const addr = "127.0.0.1:2379"

func TestBasicOperations(t *testing.T) {
	electPath := "test-operations"
	id := "test-etcd-id"
	persist, err := New(id, electPath, []string{addr})
	require.NoError(t, err)
	defer persist.Close()

	ctx := context.Background()
	keys := []string{"/a/b/c0", "/a/b/c1", "/a/b/c2"}
	value := []byte("v")
	for _, key := range keys {
		require.NoError(t, persist.Set(ctx, key, value))
		gotValue, err := persist.Get(ctx, key)
		require.NoError(t, err)
		require.Equal(t, value, gotValue)
	}
	entries, err := persist.List(ctx, "/a/b")
	require.NoError(t, err)
	require.Equal(t, len(keys), len(entries))
	for _, key := range keys {
		require.NoError(t, persist.Delete(ctx, key))
	}
}

func TestElect(t *testing.T) {
	electPath := "test-elect"
	endpoints := []string{addr}

	id0 := "test-etcd-id0"
	node0, err := New(id0, electPath, endpoints)
	require.NoError(t, err)
	require.Eventuallyf(t, func() bool {
		return node0.Leader() == node0.myID
	}, 10*time.Second, 100*time.Millisecond, "node0 should be the leader")

	id1 := "test-etcd-id1"
	node1, err := New(id1, electPath, endpoints)
	require.NoError(t, err)
	require.Eventuallyf(t, func() bool {
		return node1.Leader() == node0.myID
	}, 10*time.Second, 100*time.Millisecond, "node1's leader should be the node0")

	require.NoError(t, node0.Close())

	require.Eventuallyf(t, func() bool {
		return node1.Leader() == node1.myID
	}, 15*time.Second, 100*time.Millisecond, "node1 should be the leader")
}
