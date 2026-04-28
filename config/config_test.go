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
package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultFailOverConfig_VoteDefaults(t *testing.T) {
	cfg := DefaultFailOverConfig()
	assert.Equal(t, 2000, cfg.VoteTimeoutMs)
	assert.InDelta(t, 0.6, cfg.VoteThresholdRatio, 1e-9)
}

func TestDefaultControllerConfigSet(t *testing.T) {
	cfg := Default()
	expectedControllerConfig := &ControllerConfig{
		FailOver: &FailOverConfig{
			PingIntervalSeconds: 3,
			MaxPingCount:        5,
			VoteTimeoutMs:       2000,
			VoteThresholdRatio:  0.6,
		},
	}

	assert.Equal(t, expectedControllerConfig, cfg.Controller)
}
