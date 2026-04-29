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

package store

import "context"

// ClusterMockNode is a mock implementation of the Node interface,
// it is used for testing purposes.
type ClusterMockNode struct {
	*ClusterNode
}

var _ Node = (*ClusterMockNode)(nil)

func NewClusterMockNode() *ClusterMockNode {
	return &ClusterMockNode{
		ClusterNode: NewClusterNode("", ""),
	}
}

func (mock *ClusterMockNode) GetClusterNodeInfo(ctx context.Context) (*ClusterNodeInfo, error) {
	return &ClusterNodeInfo{
		Sequence: mock.sequence,
		Role:     mock.role,
	}, nil
}

func (mock *ClusterMockNode) GetClusterInfo(ctx context.Context) (*ClusterInfo, error) {
	return &ClusterInfo{
		CurrentEpoch:   int64(mock.sequence),
		MigratingSlot:  mock.migratingSlot,
		MigratingState: mock.migratingState,
	}, nil
}

func (mock *ClusterMockNode) MigrateSlot(ctx context.Context, slot SlotRange, targetNodeID string) error {
	mock.migratingSlot = FromSlotRange(slot)
	mock.migratingState = "start"
	return nil
}

func (mock *ClusterMockNode) GetClusterNodesString(ctx context.Context) (string, error) {
	return mock.id + " " + mock.addr + " master - 0 0 1 connected 0-16383", nil
}

func (mock *ClusterMockNode) CheckClusterMode(ctx context.Context) (int64, error) {
	return -1, nil
}

func (mock *ClusterMockNode) SyncClusterInfo(ctx context.Context, cluster *Cluster) error {
	return mock.ClusterNode.SyncClusterInfo(ctx, cluster)
}

func (mock *ClusterMockNode) Reset(ctx context.Context) error {
	return mock.ClusterNode.Reset(ctx)
}

func (mock *ClusterMockNode) MarshalJSON() ([]byte, error) {
	return mock.ClusterNode.MarshalJSON()
}

func (mock *ClusterMockNode) UnmarshalJSON(data []byte) error {
	return mock.ClusterNode.UnmarshalJSON(data)
}
