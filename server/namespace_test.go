/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package server

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/apache/kvrocks-controller/config"
	"github.com/google/uuid"
	"github.com/steinfletcher/apitest"
	"github.com/stretchr/testify/require"
)

func newTestServer() (*Server, func() *apitest.APITest) {
	server, _ := NewServer(config.Default())
	server.initHandlers()
	return server, func() *apitest.APITest {
		return apitest.New().Handler(server.engine)
	}
}

func createTestNamespace(t *testing.T, ns string) {
	server, newRequest := newTestServer()
	require.NotEmpty(t, server)
	newRequest().
		Post("/api/v1/namespaces").
		Body(fmt.Sprintf(`{"namespace": "%s"}`, ns)).
		Expect(t).
		Status(http.StatusCreated).
		End()
}

func TestNamespace(t *testing.T) {
	uri := "/api/v1/namespaces"
	ns := uuid.New().String()

	t.Run("Create namespace", func(t *testing.T) {
		createTestNamespace(t, ns)
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
