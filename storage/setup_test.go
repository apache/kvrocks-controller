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
package storage

import (
	"os"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var testEtcdClient *clientv3.Client

func setup() (err error) {
	testEtcdClient, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{"0.0.0.0:2379"},
		DialTimeout: 5 * time.Second,
	})
	return err
}

func TestMain(m *testing.M) {
	if err := setup(); err != nil {
		panic("Failed to setup the etcd client: " + err.Error())
	}
	os.Exit(m.Run())
}
