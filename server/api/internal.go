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

package api

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/apache/kvrocks-controller/controller"
)

// InternalHandler handles controller-to-controller internal RPC endpoints.
// These routes must NOT be gated by the leader-redirect middleware.
type InternalHandler struct {
	ctrl *controller.Controller
}

func NewInternalHandler(ctrl *controller.Controller) *InternalHandler {
	return &InternalHandler{ctrl: ctrl}
}

// Vote responds to a failover vote request from the leader controller.
// POST /internal/vote
func (h *InternalHandler) Vote(c *gin.Context) {
	var req controller.VoteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	checker, err := h.ctrl.GetClusterChecker(req.Namespace, req.ClusterName)
	if err != nil {
		// This node has no checker for the cluster — abstain rather than veto.
		c.JSON(http.StatusOK, controller.VoteResponse{
			Vote:   true,
			Reason: "no local checker, abstain",
		})
		return
	}

	c.JSON(http.StatusOK, checker.ShouldVote(req.FailedNodeID))
}
