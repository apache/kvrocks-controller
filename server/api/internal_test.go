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

package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/kvrocks-controller/controller"
	"github.com/apache/kvrocks-controller/server/api"
	"github.com/apache/kvrocks-controller/store"
	"github.com/apache/kvrocks-controller/store/engine"
)

func TestVoteHandler_NoChecker_Abstains(t *testing.T) {
	gin.SetMode(gin.TestMode)

	s := store.NewClusterStore(engine.NewMock())
	ctrl, err := controller.New(s, nil)
	require.NoError(t, err)

	h := api.NewInternalHandler(ctrl)
	r := gin.New()
	r.POST("/internal/vote", h.Vote)

	body, _ := json.Marshal(controller.VoteRequest{
		Namespace:    "ns",
		ClusterName:  "missing-cluster",
		ShardIndex:   0,
		FailedNodeID: "node-1",
	})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/internal/vote", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp controller.VoteResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.True(t, resp.Vote, "no checker should abstain (true)")
}
