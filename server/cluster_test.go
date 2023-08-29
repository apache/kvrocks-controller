package server

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/RocksLabs/kvrocks_controller/consts"
	"github.com/RocksLabs/kvrocks_controller/metadata"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func createTestCluster(t *testing.T, ns, clusterName string) {
	clusterURI := fmt.Sprintf("%s/%s/clusters", "/api/v1/namespaces", ns)
	server, newRequest := newTestServer()
	require.NotEmpty(t, server)
	newRequest().
		Post(clusterURI).
		Header(consts.HeaderDontDetectHost, "true").
		JSON(CreateClusterRequest{
			Name: clusterName,
			Nodes: []string{
				"1.1.1.1:6666",
				"1.1.1.2:6666",
				"1.1.1.3:6666",
			},
			Replicas: 1,
		}).
		Expect(t).
		Status(http.StatusCreated).
		End()
}

func TestCluster(t *testing.T) {
	ns := uuid.NewString()
	clusterURI := fmt.Sprintf("%s/%s/clusters", "/api/v1/namespaces", ns)
	clusterName := uuid.NewString()

	createTestNamespace(t, ns)

	t.Run("Create cluster", func(t *testing.T) {
		createTestCluster(t, ns, clusterName)
	})

	t.Run("List clusters", func(t *testing.T) {
		server, newRequest := newTestServer()
		require.NotEmpty(t, server)

		var result struct {
			Data struct {
				Clusters []string `json:"clusters"`
			}
		}
		newRequest().
			Get(clusterURI).
			Expect(t).
			Status(http.StatusOK).
			End().
			JSON(&result)
		require.Contains(t, result.Data.Clusters, clusterName)
	})

	t.Run("Get cluster", func(t *testing.T) {
		server, newRequest := newTestServer()
		require.NotEmpty(t, server)

		var result struct {
			Data struct {
				Cluster metadata.Cluster `json:"cluster"`
			}
		}
		newRequest().
			Get(clusterURI + "/" + clusterName).
			Expect(t).
			Status(http.StatusOK).
			End().
			JSON(&result)
		require.Equal(t, clusterName, result.Data.Cluster.Name)
		require.Equal(t, 3, len(result.Data.Cluster.Shards))
	})

	t.Run("Get non-exists cluster", func(t *testing.T) {
		server, newRequest := newTestServer()
		require.NotEmpty(t, server)

		var result struct {
			Data struct {
				Cluster metadata.Cluster `json:"cluster"`
			}
		}
		newRequest().
			Get(clusterURI + "/non-exists-cluster").
			Expect(t).
			Status(http.StatusNotFound).
			End().
			JSON(&result)
	})

	t.Run("Delete cluster", func(t *testing.T) {
		server, newRequest := newTestServer()
		require.NotEmpty(t, server)
		newRequest().
			Delete(clusterURI + "/" + clusterName).
			Expect(t).
			Status(http.StatusOK).
			End()
	})
}
