package server

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/KvrocksLabs/kvrocks_controller/consts"
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestNodeHandler(t *testing.T) {
	ns := uuid.NewString()
	clusterName := uuid.NewString()
	nodeURI := fmt.Sprintf("/api/v1/namespaces/%s/clusters/%s/shards/0/nodes", ns, clusterName)
	nodeID := util.GenerateNodeID()
	createTestNamespace(t, ns)
	createTestCluster(t, ns, clusterName)

	t.Run("Create node", func(t *testing.T) {
		server, newRequest := newTestServer()
		require.NotEmpty(t, server)
		newRequest().
			Post(nodeURI).
			Header(consts.HeaderDontDetectHost, "true").
			JSON(metadata.NodeInfo{
				ID:   nodeID,
				Addr: "1.1.1.5:1234",
				Role: metadata.RoleSlave,
			}).
			Expect(t).
			Status(http.StatusCreated).
			End()
	})

	t.Run("List node", func(t *testing.T) {
		server, newRequest := newTestServer()
		require.NotEmpty(t, server)

		var result struct {
			Data struct {
				Nodes []metadata.NodeInfo `json:"nodes"`
			}
		}
		newRequest().
			Get(nodeURI).
			Expect(t).
			Status(http.StatusOK).
			End().
			JSON(&result)
		require.Equal(t, 2, len(result.Data.Nodes))
	})

	t.Run("Delete node", func(t *testing.T) {
		server, newRequest := newTestServer()
		require.NotEmpty(t, server)

		newRequest().
			Delete(nodeURI + "/" + nodeID).
			Expect(t).
			Status(http.StatusNoContent).
			End()

		var result struct {
			Data struct {
				Nodes []metadata.NodeInfo `json:"nodes"`
			}
		}
		newRequest().
			Get(nodeURI).
			Expect(t).
			Status(http.StatusOK).
			End().
			JSON(&result)
		require.Equal(t, 1, len(result.Data.Nodes))
	})
}
