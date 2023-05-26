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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig(t *testing.T) {
	cfg := Config{}

	cfg.Addr = ""
	cfg.init()
	t.Log(cfg.Addr) // 172.16.40.81:9379

	cfg.Addr = ":8080"
	cfg.init()
	t.Log(cfg.Addr) // 172.16.40.81:8080

	addr := "1.1.1.1:8080"
	cfg.Addr = addr
	cfg.init()
	assert.Equal(t, addr, cfg.Addr)

	os.Setenv("KVROCKS_CONTROLLER_HTTP_HOST", "1.2.3.4")
	os.Setenv("KVROCKS_CONTROLLER_HTTP_PORT", "8080")
	cfg.init()
	assert.Equal(t, "1.2.3.4:8080", cfg.Addr)

	// unset env, avoid environmental pollution
	os.Setenv("KVROCKS_CONTROLLER_HTTP_HOST", "")
	os.Setenv("KVROCKS_CONTROLLER_HTTP_PORT", "")
}
