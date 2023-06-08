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
	"net"
	"os"

	"github.com/go-playground/validator/v10"

	"github.com/RocksLabs/kvrocks_controller/storage/persistence/etcd"
)

type AdminConfig struct {
	Addr string `yaml:"addr"`
}

const defaultPort = 9379

type WebConfig struct {
	Dir string `yaml:"dir"`
}

type Config struct {
	Addr  string       `yaml:"addr"`
	Etcd  *etcd.Config `yaml:"etcd"`
	Admin AdminConfig  `yaml:"admin"`
	Web   *WebConfig   `yaml:"web"`
}

func (c *Config) init() {
	if c == nil {
		*c = Config{}
	}

	c.Addr = c.getAddr()
	if c.Etcd == nil {
		c.Etcd = &etcd.Config{
			Addrs: []string{"127.0.0.1:2379"},
		}
	}
	if c.Web == nil {
		c.Web = &WebConfig{
			Dir: "./web",
		}
	}
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
