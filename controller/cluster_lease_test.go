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
package controller

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/kvrocks-controller/config"
	"github.com/apache/kvrocks-controller/store"
	"github.com/apache/kvrocks-controller/store/engine"
)

func newTestChecker() *ClusterChecker {
	return NewClusterChecker(store.NewClusterStore(engine.NewMock()), "ns", "cluster")
}

func TestWithLeaseConfig_Nil(t *testing.T) {
	c := newTestChecker()
	c.WithLeaseConfig(nil)
	require.Nil(t, c.leaseConfig, "leaseConfig must remain nil when passed nil")
}

func TestWithLeaseConfig_Disabled(t *testing.T) {
	c := newTestChecker()
	c.WithLeaseConfig(&config.LeaseConfig{Enabled: false, LeaseMs: 500})
	require.Nil(t, c.leaseConfig, "leaseConfig must remain nil when Enabled=false")
}

func TestWithLeaseConfig_Enabled(t *testing.T) {
	c := newTestChecker()
	cfg := &config.LeaseConfig{Enabled: true, LeaseMs: 500}
	c.WithLeaseConfig(cfg)
	require.NotNil(t, c.leaseConfig)
	require.Equal(t, cfg, c.leaseConfig)
}
