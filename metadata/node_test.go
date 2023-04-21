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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeInfo_Validate(t *testing.T) {
	node := &NodeInfo{}
	require.EqualError(t, node.Validate(), "node id shouldn't be empty")
	node.ID = "1234"
	require.EqualError(t, node.Validate(), "the length of node id must be 40")
	node.ID = strings.Repeat("1", NodeIdLen)
	require.EqualError(t, node.Validate(), "node role should be 'master' or 'slave'")
	node.Role = RoleMaster
	node.Addr = "1.2.3.4"
	require.NoError(t, node.Validate())
}
