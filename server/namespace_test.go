package server

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/steinfletcher/apitest"
	"github.com/stretchr/testify/require"
)

func newTestServer() (*Server, func() *apitest.APITest) {
	server, _ := NewServer(&Config{
		Etcd: &EtcdConfig{
			Addrs: []string{"127.0.0.1:2379"},
		},
	})
	server.initHandlers()
	return server, func() *apitest.APITest {
		return apitest.New().Handler(server.engine)
	}
}

func TestNamespace(t *testing.T) {
	uri := "/api/v1/namespaces"
	ns := uuid.New().String()
	t.Run("Create namespace", func(t *testing.T) {
		server, newRequest := newTestServer()
		require.NotEmpty(t, server)
		newRequest().
			Post(uri).
			Body(fmt.Sprintf(`{"namespace": "%s"}`, ns)).
			Expect(t).
			Status(http.StatusCreated).
			End()
	})

	t.Run("Create duplicate namespace", func(t *testing.T) {
		server, newRequest := newTestServer()
		require.NotEmpty(t, server)
		newRequest().
			Post(uri).
			Body(fmt.Sprintf(`{"namespace": "%s"}`, ns)).
			Expect(t).
			Status(http.StatusConflict).
			End()
	})

	t.Run("List namespaces", func(t *testing.T) {
		var result struct {
			Data struct {
				Namespaces []string `json:"namespaces"`
			} `json:"data"`
		}
		server, newRequest := newTestServer()
		require.NotEmpty(t, server)
		newRequest().
			Get(uri).
			Expect(t).
			Status(http.StatusOK).
			End().
			JSON(&result)
		require.Contains(t, result.Data.Namespaces, ns)
	})

	t.Run("Delete namespace", func(t *testing.T) {
		server, newRequest := newTestServer()
		require.NotEmpty(t, server)
		newRequest().
			Delete(uri + "/" + ns).
			Expect(t).
			Status(http.StatusOK).
			End()

		// double delete should return 404
		newRequest().
			Delete(uri + "/" + ns).
			Expect(t).
			Status(http.StatusNotFound).
			End()
	})
}
