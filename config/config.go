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
	"errors"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/apache/kvrocks-controller/store/engine/consul"
	"github.com/apache/kvrocks-controller/store/engine/raft"

	"github.com/go-playground/validator/v10"

	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/store/engine/etcd"
	"github.com/apache/kvrocks-controller/store/engine/zookeeper"
)

type AdminConfig struct {
	Addr string `yaml:"addr"`
}

type FailOverConfig struct {
	PingIntervalSeconds int   `yaml:"ping_interval_seconds"`
	MaxPingCount        int64 `yaml:"max_ping_count"`
	// EnableSlaveHAUpdate controls whether HA logic marks failed slave nodes and
	// propagates the updated topology. Requires kvrocks to support node status
	// modification (new versions only). Defaults to false for backward compatibility.
	EnableSlaveHAUpdate bool    `yaml:"enable_slave_ha_update"`
	WaitForSync         bool    `yaml:"wait_for_sync"`
	VoteTimeoutMs       int     `yaml:"vote_timeout_ms"`
	VoteThresholdRatio  float64 `yaml:"vote_threshold_ratio"`
}

type ControllerConfig struct {
	FailOver *FailOverConfig `yaml:"failover"`
}

type LogConfig struct {
	Level      string `yaml:"level"`
	Filename   string `yaml:"filename"`
	MaxBackups int    `yaml:"max_backups"`
	MaxAge     int    `yaml:"max_age"`
	MaxSize    int    `yaml:"max_size"`
	Compress   bool   `yaml:"compress"`
}

const defaultPort = 9379

type Config struct {
	Addr        string            `yaml:"addr"`
	StorageType string            `yaml:"storage_type"`
	Etcd        *etcd.Config      `yaml:"etcd"`
	Zookeeper   *zookeeper.Config `yaml:"zookeeper"`
	Raft        *raft.Config      `yaml:"raft"`
	Consul      *consul.Config    `yaml:"consul"`
	Admin       AdminConfig       `yaml:"admin"`
	Controller  *ControllerConfig `yaml:"controller"`
	Log         *LogConfig        `yaml:"log"`
}

func DefaultFailOverConfig() *FailOverConfig {
	return &FailOverConfig{
		PingIntervalSeconds: 3,
		MaxPingCount:        5,
		VoteTimeoutMs:       2000,
		VoteThresholdRatio:  0.6,
	}
}

func Default() *Config {
	c := &Config{
		Etcd: &etcd.Config{
			Addrs: []string{"127.0.0.1:2379"},
		},
		Controller: &ControllerConfig{
			FailOver: DefaultFailOverConfig(),
		},
	}
	c.Addr = c.getAddr()
	return c
}

func (c *Config) Validate() error {
	if c.Controller.FailOver.MaxPingCount < 3 {
		return errors.New("max ping count required >= 3")
	}
	if c.Controller.FailOver.PingIntervalSeconds < 1 {
		return errors.New("ping interval required >= 1s")
	}
	if c.Controller.FailOver.VoteTimeoutMs < 1 {
		return errors.New("vote_timeout_ms required >= 1 (0 causes all peer votes to time out immediately)")
	}
	if c.Controller.FailOver.VoteThresholdRatio <= 0 || c.Controller.FailOver.VoteThresholdRatio > 1.0 {
		return errors.New("vote_threshold_ratio must be in range (0, 1.0]")
	}
	hostPort := strings.Split(c.Addr, ":")
	if hostPort[0] == "0.0.0.0" || hostPort[0] == "127.0.0.1" {
		logger.Get().Warn("Leader forward may not work if the host is " + hostPort[0])
	}
	return nil
}

func (c *Config) getAddr() string {
	// env has higher priority than configuration.
	// case: get addr from env
	checker := validator.New()
	host := os.Getenv("KVROCKS_CONTROLLER_HTTP_HOST")
	port := os.Getenv("KVROCKS_CONTROLLER_HTTP_PORT")
	addr := host + ":" + port
	err := checker.Var(addr, "required,tcp_addr")
	if err == nil {
		return fmt.Sprintf("%s:%s", host, port)
	}
	if c.Addr != "" {
		return c.Addr
	}

	// case: addr is empty
	ip := getLocalIP()
	if ip != "" {
		return fmt.Sprintf("%s:%d", ip, defaultPort)
	}
	return fmt.Sprintf("127.0.0.1:%d", defaultPort)
}

// getLocalIP returns the non loopback local IP of the host.
func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		ipnet, ok := address.(*net.IPNet)
		if ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}
