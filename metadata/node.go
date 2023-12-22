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
package metadata

import (
	"errors"

	"github.com/go-playground/validator/v10"
)

const (
	RoleMaster = "master"
	RoleSlave  = "slave"
)

var (
	NodeIdLen = 40
)

var _validator = validator.New()

type NodeInfo struct {
	ID        string `json:"id" validate:"required"`
	Addr      string `json:"addr" validate:"required"`
	Role      string `json:"role" validate:"required"`
	Password  string `json:"password"`
	CreatedAt int64  `json:"created_at"`
}

func (nodeInfo *NodeInfo) Validate() error {
	if len(nodeInfo.ID) == 0 {
		return errors.New("node id shouldn't be empty")
	}
	if len(nodeInfo.ID) != NodeIdLen {
		return errors.New("the length of node id must be 40")
	}
	if nodeInfo.Role != RoleMaster && nodeInfo.Role != RoleSlave {
		return errors.New("node role should be 'master' or 'slave'")
	}
	// TODO: check the node address format
	return _validator.Struct(nodeInfo)
}

func (nodeInfo *NodeInfo) IsMaster() bool {
	return nodeInfo.Role == RoleMaster
}
