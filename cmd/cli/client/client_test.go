package client

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/server/handlers"

	"github.com/google/uuid"

	"github.com/stretchr/testify/require"
)

func TestClient(t *testing.T) {
	ctx := context.TODO()
	testNS := uuid.New().String()
	clusterName := "test-cluster"

	c := New("")
	// Namespace
	namespaces, err := c.ListNamespace(ctx)
	require.NoError(t, err)
	for _, namespace := range namespaces {
		require.NoError(t, c.DeleteNamespace(ctx, namespace))
	}
	namespaces, err = c.ListNamespace(ctx)
	require.NoError(t, err)
	require.Empty(t, namespaces)
	require.NoError(t, c.CreateNamespace(ctx, testNS))

	// Cluster
	clusterRequest := handlers.CreateClusterRequest{
		Cluster: clusterName,
		Shards: []handlers.CreateShardRequest{
			{Master: &metadata.NodeInfo{Address: "127.0.0.1:6666", ID: "0123456789012345678901234567890123456789"}},
		},
	}
	require.NoError(t, c.CreateCluster(ctx, testNS, &clusterRequest))
	clusterInfo, err := c.GetCluster(ctx, testNS, clusterName)
	require.NoError(t, err)
	require.Equal(t, 1, len(clusterInfo.Shards))
	assert.NoError(t, c.CreateClusterShard(ctx, testNS, clusterName, &handlers.CreateShardRequest{
		Master: &metadata.NodeInfo{
			Address: "127.0.0.1:6667",
			ID:      "0123456789012345678901234567890123456789",
		},
	}))
	clusterInfo, err = c.GetCluster(ctx, testNS, clusterName)
	require.NoError(t, err)
	require.Equal(t, 2, len(clusterInfo.Shards))

	require.NoError(t, c.RemoveClusterShard(ctx, testNS, clusterName, 1))
	clusterInfo, err = c.GetCluster(ctx, testNS, clusterName)
	require.NoError(t, err)
	require.Equal(t, 1, len(clusterInfo.Shards))

	err = c.CreateClusterNode(ctx, testNS, clusterName, 0, &metadata.NodeInfo{
		ID:      "u123456789012345678901234567890123456789",
		Address: "127.0.0.1:6668",
		Role:    "slave",
	})
	require.NoError(t, err)
	shard, err := c.GetClusterShard(ctx, testNS, clusterName, 0)
	require.NoError(t, err)
	require.Equal(t, 2, len(shard.Nodes))
	require.NoError(t, c.RemoveCluster(ctx, testNS, clusterName))
}
