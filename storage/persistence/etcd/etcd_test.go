package etcd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

const addr = "127.0.0.1:2379"

func TestBasicOperations(t *testing.T) {
	persist, err := New([]string{addr})
	require.NoError(t, err)

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
