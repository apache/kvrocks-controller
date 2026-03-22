//go:build integration

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
package integration

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
)

var binPath = flag.String("binPath", "", "path to the kvrocks binary")

// KvrocksHandle represents a running kvrocks process for integration tests.
type KvrocksHandle struct {
	t      testing.TB
	cmd    *exec.Cmd
	addr   string
	logDir string
}

// Addr returns host:port of the running kvrocks instance.
func (h *KvrocksHandle) Addr() string { return h.addr }

// Client returns a new redis client connected to this instance.
func (h *KvrocksHandle) Client() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         h.addr,
		DialTimeout:  10 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	})
}

// ConfigSet runs CONFIG SET key value on the running instance.
func (h *KvrocksHandle) ConfigSet(key, value string) {
	h.t.Helper()
	ctx := context.Background()
	c := h.Client()
	defer c.Close()
	require.NoError(h.t, c.Do(ctx, "CONFIG", "SET", key, value).Err())
}

// LogPath returns the path to today's kvrocks log file.
// kvrocks uses spdlog daily_file_sink_mt: <log-dir>/kvrocks_YYYY-MM-DD.log
func (h *KvrocksHandle) LogPath() string {
	now := time.Now()
	return filepath.Join(h.logDir, fmt.Sprintf("kvrocks_%d-%02d-%02d.log",
		now.Year(), now.Month(), now.Day()))
}

// startKvrocks forks a kvrocks process with the given config key-value pairs.
// The process is killed automatically via t.Cleanup.
func startKvrocks(t testing.TB, configs map[string]string) *KvrocksHandle {
	t.Helper()
	b := *binPath
	require.NotEmpty(t, b, "pass -binPath=/path/to/kvrocks to run integration tests")

	dir := t.TempDir()
	logDir := filepath.Join(dir, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0755))

	// Find a free port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	ln.Close()

	// Merge caller-supplied configs with required defaults.
	merged := map[string]string{
		"bind":              "127.0.0.1",
		"port":              fmt.Sprintf("%d", ln.Addr().(*net.TCPAddr).Port),
		"dir":               dir,
		"log-dir":           logDir,
		"master-lease-mode": "disabled",
	}
	for k, v := range configs {
		merged[k] = v
	}

	// Write kvrocks.conf.
	confPath := filepath.Join(dir, "kvrocks.conf")
	f, err := os.Create(confPath)
	require.NoError(t, err)
	for k, v := range merged {
		_, err = fmt.Fprintf(f, "%s %s\n", k, v)
		require.NoError(t, err)
	}
	require.NoError(t, f.Close())

	cmd := exec.Command(b, "-c", confPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())

	// Wait until kvrocks is ready to accept connections (up to 30s).
	c := redis.NewClient(&redis.Options{Addr: addr})
	require.Eventually(t, func() bool {
		return c.Ping(context.Background()).Err() == nil
	}, 30*time.Second, 200*time.Millisecond, "kvrocks did not start in time")
	require.NoError(t, c.Close())

	h := &KvrocksHandle{t: t, cmd: cmd, addr: addr, logDir: logDir}

	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	return h
}

// initCluster puts the kvrocks instance into single-node cluster mode.
// nodeID must be exactly 40 hex characters.
func initCluster(t testing.TB, h *KvrocksHandle, nodeID string) {
	t.Helper()
	ctx := context.Background()
	c := h.Client()
	defer c.Close()

	require.NoError(t, c.Do(ctx, "CLUSTERX", "SETNODEID", nodeID).Err())
	host, port, err := net.SplitHostPort(h.Addr())
	require.NoError(t, err)
	clusterNodes := fmt.Sprintf("%s %s %s master - 0-16383", nodeID, host, port)
	require.NoError(t, c.Do(ctx, "CLUSTERX", "SETNODES", clusterNodes, "1").Err())
}
