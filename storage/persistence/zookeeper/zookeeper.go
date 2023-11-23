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
package zookeeper

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/storage/persistence"
	"github.com/go-zookeeper/zk"
	"go.uber.org/atomic"
)

const (
	sessionTTL         = 6
	defaultDailTimeout = 5 * time.Second
)

const defaultElectPath = "/leader"

type Config struct {
	Addrs    []string `yaml:"addrs"`
	Username string   `yaml:"username"`
	Password string   `yaml:"password"`
	TLS      struct {
		Enable        bool   `yaml:"enable"`
		CertFile      string `yaml:"cert_file"`
		KeyFile       string `yaml:"key_file"`
		TrustedCAFile string `yaml:"ca_file"`
	} `yaml:"tls"`
	ElectPath string `yaml:"elect_path"`
}

type Zk struct {
	conn *zk.Conn

	leaderMu       sync.RWMutex
	leaderID       string
	myID           string
	electPath      string
	isReady        atomic.Bool
	quitCh         chan struct{}
	leaderChangeCh chan bool
}

func New(id string, cfg *Config) (*Zk, error) {
	if len(id) == 0 {
		return nil, errors.New("id must NOT be a empty string")
	}
	client, _, err := zk.Connect(cfg.Addrs, defaultDailTimeout)

	if err != nil {
		return nil, err
	}

	electPath := defaultElectPath
	// if cfg.ElectPath != "" {
	// 	electPath = cfg.ElectPath
	// }
	e := &Zk{
		myID:           id,
		electPath:      electPath,
		conn:           client,
		quitCh:         make(chan struct{}),
		leaderChangeCh: make(chan bool),
	}
	e.isReady.Store(false)
	go e.observeLeaderEvent(context.Background())
	return e, nil
}

func (e *Zk) ID() string {
	return e.myID
}

func (e *Zk) Leader() string {
	e.leaderMu.RLock()
	defer e.leaderMu.RUnlock()
	return e.leaderID
}

func (e *Zk) LeaderChange() <-chan bool {
	return e.leaderChangeCh
}

func (e *Zk) IsReady(ctx context.Context) bool {
	for {
		select {
		case <-e.quitCh:
			return false
		case <-time.After(100 * time.Millisecond):
			if e.isReady.Load() {
				return true
			}
		case <-ctx.Done():
			return e.isReady.Load()
		}
	}
}

func (c *Zk) Get(ctx context.Context, key string) ([]byte, error) {
	data, _, err := c.conn.Get(key)
	if err != nil {
		if err == zk.ErrNoNode {
			return nil, nil // Key does not exist
		}
		return nil, err
	}

	return data, nil
}

func (c *Zk) Exists(ctx context.Context, key string) (bool, error) {
	exists, _, err := c.conn.Exists(key)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (c *Zk) Set(ctx context.Context, key string, value []byte) error {
	exist, _ := c.Exists(ctx, key)
	if exist {
		_, err := c.conn.Set(key, value, -1)
		return err
	}

	return c.Create(ctx, key, value)
}

func (c *Zk) Create(ctx context.Context, key string, value []byte) error {
	lastSlashIndex := strings.LastIndex(key, "/")
	if lastSlashIndex != 0 {
		substring := key[:lastSlashIndex]
		// 如果父节点不存在，依次创建父节点
		exist, _ := c.Exists(ctx, substring)
		if !exist {
			c.Create(ctx, substring, []byte{})
		}
	}
	acls := zk.WorldACL(zk.PermAll)
	_, err := c.conn.Create(key, value, 0, acls)
	if err != nil {
		println("return", key, err.Error())
	}

	return err
}

func (c *Zk) Delete(ctx context.Context, key string) error {
	err := c.conn.Delete(key, -1)
	if err == zk.ErrNoNode {
		return nil // Key does not exist
	}
	return err
}

func (c *Zk) List(ctx context.Context, prefix string) ([]persistence.Entry, error) {
	children, _, err := c.conn.Children(prefix)
	if err != nil {
		return nil, err
	}

	entries := make([]persistence.Entry, 0)
	for _, child := range children {
		key := prefix + "/" + child
		data, _, err := c.conn.Get(key)
		if err != nil {
			return nil, err
		}

		entry := persistence.Entry{
			Key:   key,
			Value: data,
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

func (e *Zk) observeLeaderEvent(ctx context.Context) {
	acls := zk.WorldACL(zk.PermAll)
reset:
	select {
	case <-e.quitCh:
		return
	default:
	}

	_, err := e.conn.Create(e.electPath, []byte(e.myID), zk.FlagEphemeral, acls)
	println("create", e.electPath)
	data, _, ch, err := e.conn.GetW(e.electPath)
	if err != nil {
		println("GetW reset", err.Error())
		goto reset
	}
	if string(data) != "" && string(data) != e.leaderID {
		println("leaderID ", string(data))
		e.leaderMu.Lock()
		e.leaderID = string(data)
		e.leaderMu.Unlock()
	}
	for {
		select {
		case resp := <-ch:
			println("yes ", zk.EventNodeDeleted)
			if resp.Type == zk.EventNodeDeleted {
				e.conn.Create(e.electPath, []byte{}, zk.FlagEphemeral, acls)
			}
			data, _, ch, err = e.conn.GetW(e.electPath)
			if err != nil {
				goto reset
			}
			if string(data) != "" && string(data) != e.leaderID {
				e.leaderMu.Lock()
				e.leaderID = string(data)
				e.leaderMu.Unlock()
				e.leaderChangeCh <- true
			}
		case <-e.quitCh:
			logger.Get().Info("Exit the leader election loop")
			return
		}

	}

}

func (e *Zk) Close() error {
	close(e.quitCh)
	e.conn.Close()
	return nil
}
