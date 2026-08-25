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

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/apache/kvrocks-controller/logger"

	"github.com/apache/kvrocks-controller/consts"
)

const (
	// the old migrating slot was denoted by an int and -1 was
	// used to denote a non migrating slot
	NotMigratingInt = -1
)

// FailoverOptions configures manual failover behavior.
type FailoverOptions struct {
	WaitForSync    bool          // whether to wait for replication gap to reach 0
	SyncTimeout    time.Duration // max wait time for gap to reach 0
	PauseDuration  time.Duration // CLIENT PAUSE timeout parameter, must be > SyncTimeout
	ForceOnTimeout bool          // if true, proceed with failover on sync timeout
	PollInterval   time.Duration // interval between INFO replication polls
	PollTimeout    time.Duration // deadline for one poll cycle (two INFO replication RPCs: old master + target replica)
}

// DefaultFailoverOptions returns default options for manual failover.
// WaitForSync is disabled by default to maintain compatibility with older kvrocks versions
// that do not support CLIENT PAUSE/UNPAUSE. Set WaitForSync=true via the API options field
// when targeting kvrocks instances that support these commands.
func DefaultFailoverOptions() FailoverOptions {
	return FailoverOptions{
		WaitForSync:    false,
		SyncTimeout:    100 * time.Millisecond,
		PauseDuration:  500 * time.Millisecond,
		ForceOnTimeout: false,
		PollInterval:   10 * time.Millisecond,
		PollTimeout:    40 * time.Millisecond,
	}
}

type Shard struct {
	Nodes            []Node         `json:"nodes"`
	SlotRanges       []SlotRange    `json:"slot_ranges"`
	TargetShardIndex int            `json:"target_shard_index"`
	MigratingSlot    *MigratingSlot `json:"migrating_slot"`
}

type Shards []*Shard

func (s Shards) Len() int {
	return len(s)
}

func (s Shards) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s Shards) Less(i, j int) bool {
	if len(s[i].SlotRanges) == 0 {
		return false
	} else if len(s[j].SlotRanges) == 0 {
		return true
	}
	return s[i].SlotRanges[0].Start < s[j].SlotRanges[0].Start
}

func NewShard() *Shard {
	return &Shard{
		Nodes:            make([]Node, 0),
		SlotRanges:       make([]SlotRange, 0),
		MigratingSlot:    nil,
		TargetShardIndex: -1,
	}
}

func (shard *Shard) Clone() *Shard {
	clone := NewShard()
	clone.SlotRanges = make([]SlotRange, len(shard.SlotRanges))
	copy(clone.SlotRanges, shard.SlotRanges)
	clone.TargetShardIndex = shard.TargetShardIndex
	clone.MigratingSlot = shard.MigratingSlot
	clone.Nodes = make([]Node, len(shard.Nodes))
	copy(clone.Nodes, shard.Nodes)
	return clone
}

func (shard *Shard) ClearMigrateState() {
	shard.MigratingSlot = nil
	shard.TargetShardIndex = -1
}

func (shard *Shard) IsServicing() bool {
	for _, slotRange := range shard.SlotRanges {
		if slotRange.Start != -1 || slotRange.Stop != -1 {
			return true
		}
	}
	return shard.IsMigrating()
}

func (shard *Shard) addNode(addr, role, password string) (*ClusterNode, error) {
	if role != RoleMaster && role != RoleSlave {
		return nil, fmt.Errorf("%w: role", consts.ErrInvalidArgument)
	}
	for _, node := range shard.Nodes {
		if node.Addr() == addr {
			return nil, consts.ErrAlreadyExists
		}
	}
	if role == RoleMaster && len(shard.Nodes) > 0 {
		return nil, fmt.Errorf("master node %w", consts.ErrAlreadyExists)
	}
	node := NewClusterNode(addr, password)
	node.SetRole(role)
	shard.Nodes = append(shard.Nodes, node)
	return node, nil
}

func (shard *Shard) IsMigrating() bool {
	return shard.MigratingSlot != nil && shard.MigratingSlot.IsMigrating && shard.TargetShardIndex != -1
}

func (shard *Shard) GetMasterNode() Node {
	for _, node := range shard.Nodes {
		if node.IsMaster() {
			return node
		}
	}
	return nil
}

func (shard *Shard) removeNode(nodeID string) error {
	isFound := false
	for i, node := range shard.Nodes {
		if node.ID() != nodeID {
			continue
		}
		if node.IsMaster() {
			return fmt.Errorf("cannot remove master node: %w", consts.ErrInvalidArgument)
		}
		shard.Nodes = append(shard.Nodes[:i], shard.Nodes[i+1:]...)
		isFound = true
	}
	if !isFound {
		return consts.ErrNotFound
	}
	return nil
}

func (shard *Shard) getNewMasterNodeIndex(ctx context.Context, masterNodeIndex int, preferredNodeID string) int {
	newMasterNodeIndex := -1
	var newestOffset uint64

	// Get master sequence to handle empty shard case (issue #366)
	var masterSequence uint64
	if masterNodeIndex >= 0 && masterNodeIndex < len(shard.Nodes) {
		masterNode := shard.Nodes[masterNodeIndex]
		if _, err := masterNode.GetClusterInfo(ctx); err == nil {
			if masterInfo, err := masterNode.GetClusterNodeInfo(ctx); err == nil {
				masterSequence = masterInfo.Sequence
			}
		}
	}

	for i, node := range shard.Nodes {
		// don't promote the current master node
		if i == masterNodeIndex {
			continue
		}

		_, err := node.GetClusterInfo(ctx)
		if err != nil {
			logger.Get().With(
				zap.Error(err),
				zap.String("id", node.ID()),
				zap.String("addr", node.Addr()),
			).Warn("Skip the node due to failed to get cluster info")
			continue
		}

		clusterNodeInfo, err := node.GetClusterNodeInfo(ctx)
		if err != nil {
			logger.Get().With(
				zap.Error(err),
				zap.String("id", node.ID()),
				zap.String("addr", node.Addr()),
			).Warn("Skip the node due to failed to get info of node")
			continue
		}
		// Fix #366: allow sequence == 0 only when master sequence is also 0 (empty shard)
		if clusterNodeInfo.Role != RoleSlave || (clusterNodeInfo.Sequence == 0 && masterSequence != 0) {
			logger.Get().With(
				zap.String("id", node.ID()),
				zap.String("addr", node.Addr()),
				zap.String("role", clusterNodeInfo.Role),
				zap.Uint64("sequence", clusterNodeInfo.Sequence),
				zap.Uint64("master_sequence", masterSequence),
			).Warn("Skip the node due to role or sequence invalid")
			continue
		}

		logger.Get().With(
			zap.String("id", node.ID()),
			zap.String("addr", node.Addr()),
			zap.String("role", clusterNodeInfo.Role),
			zap.Uint64("sequence", clusterNodeInfo.Sequence),
		).Info("Get slave node info successfully")

		// If the preferredNodeID is not empty, we will use it as the new master node.
		if preferredNodeID != "" && node.ID() == preferredNodeID {
			newMasterNodeIndex = i
			break
		}
		if clusterNodeInfo.Sequence >= newestOffset {
			newMasterNodeIndex = i
			newestOffset = clusterNodeInfo.Sequence
		}
	}
	return newMasterNodeIndex
}

// waitForReplicationSync polls INFO replication on the old master and on the target replica until
// ReplicaAppliedReplOffset(replica) >= master.MasterReplOffset, so offsets come from each process
// directly instead of the master's slave list (which can lag).
func (shard *Shard) waitForReplicationSync(ctx context.Context, oldMaster Node, targetSlave Node, opts FailoverOptions) error {
	// Bound the entire sync operation with SyncTimeout. Each poll cycle issues two concurrent INFO calls;
	// PollTimeout is the budget for that cycle (both RPCs share one deadline).
	syncCtx, syncCancel := context.WithTimeout(ctx, opts.SyncTimeout)
	defer syncCancel()

	ticker := time.NewTicker(opts.PollInterval)
	defer ticker.Stop()

	targetAddr := targetSlave.Addr()
	// waitNextTick blocks until the next poll interval or the sync deadline is exceeded.
	// Returns nil to signal the caller should continue, or a non-nil error to abort.
	waitNextTick := func() error {
		select {
		case <-syncCtx.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("%w: slave %s did not catch up within %v", consts.ErrSyncTimeout, targetAddr, opts.SyncTimeout)
		case <-ticker.C:
			return nil
		}
	}

	for {
		pollCtx, cancel := context.WithTimeout(syncCtx, opts.PollTimeout)
		var masterInfo, slaveInfo *ReplicationInfo
		var errM, errS error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			masterInfo, errM = oldMaster.GetReplicationInfo(pollCtx)
		}()
		go func() {
			defer wg.Done()
			slaveInfo, errS = targetSlave.GetReplicationInfo(pollCtx)
		}()
		wg.Wait()
		cancel()
		if errM != nil || errS != nil {
			if errM != nil {
				logger.Get().With(
					zap.Error(errM),
					zap.String("master", oldMaster.Addr()),
				).Warn("Failed to get replication info from old master, will retry")
			}
			if errS != nil {
				logger.Get().With(
					zap.Error(errS),
					zap.String("slave", targetAddr),
				).Warn("Failed to get replication info from target replica, will retry")
			}
			if err := waitNextTick(); err != nil {
				return err
			}
			continue
		}

		if masterInfo.Role != RoleMaster {
			return fmt.Errorf("node %s is not master (role=%s)", oldMaster.Addr(), masterInfo.Role)
		}
		if slaveInfo.Role != RoleSlave {
			return fmt.Errorf("node %s is not slave (role=%s)", targetAddr, slaveInfo.Role)
		}
		if slaveInfo.MasterLinkStatus != "" && !strings.EqualFold(slaveInfo.MasterLinkStatus, "up") {
			return fmt.Errorf("replication link for %s is not up (master_link_status=%s)", targetAddr, slaveInfo.MasterLinkStatus)
		}

		masterOff := masterInfo.MasterReplOffset
		slaveOff := ReplicaAppliedReplOffset(slaveInfo)
		if slaveOff >= masterOff {
			return nil
		}

		if err := waitNextTick(); err != nil {
			return err
		}
	}
}

// promoteNewMaster promotes a new master node in the shard.
// It returns oldMasterNode and newMasterNode for the handler to orchestrate
// UpdateCluster, SyncClusterInfo, and UnpauseClient.
//
// The masterNodeID is used to check if the node is the current master node if it's not empty.
// The preferredNodeID is used to specify the preferred node to be promoted as the new master node,
// it will choose the node with the highest sequence number if the preferredNodeID is empty.
//
// When WaitForSync is true, it will CLIENT PAUSE the old master, wait for replication gap to reach 0,
// then modify roles. The handler must call UnpauseClient on oldMaster after UpdateCluster and push.
func (shard *Shard) promoteNewMaster(ctx context.Context, masterNodeID, preferredNodeID string, opts FailoverOptions) (
	oldMasterNode Node, newMasterNode Node, err error) {
	if len(shard.Nodes) <= 1 {
		return nil, nil, consts.ErrShardNoReplica
	}

	oldMasterNodeIndex := -1
	for i, node := range shard.Nodes {
		if node.IsMaster() {
			oldMasterNodeIndex = i
			break
		}
	}
	if oldMasterNodeIndex == -1 {
		return nil, nil, consts.ErrOldMasterNodeNotFound
	}
	if masterNodeID != "" && shard.Nodes[oldMasterNodeIndex].ID() != masterNodeID {
		return nil, nil, consts.ErrNodeIsNotMaster
	}
	newMasterNodeIndex := shard.getNewMasterNodeIndex(ctx, oldMasterNodeIndex, preferredNodeID)
	if newMasterNodeIndex == -1 {
		return nil, nil, consts.ErrShardNoMatchNewMaster
	}

	oldMaster := shard.Nodes[oldMasterNodeIndex]
	newMaster := shard.Nodes[newMasterNodeIndex]

	if opts.WaitForSync {
		if opts.PauseDuration <= opts.SyncTimeout {
			return nil, nil, fmt.Errorf("PauseDuration (%v) must be greater than SyncTimeout (%v)", opts.PauseDuration, opts.SyncTimeout)
		}
		if err = oldMaster.PauseClient(ctx, opts.PauseDuration); err != nil {
			return nil, nil, fmt.Errorf("CLIENT PAUSE failed: %w", err)
		}
		defer func() {
			if err != nil {
				_ = oldMaster.UnpauseClient(ctx)
			}
		}()

		syncErr := shard.waitForReplicationSync(ctx, oldMaster, newMaster, opts)
		if syncErr != nil {
			if opts.ForceOnTimeout && errors.Is(syncErr, consts.ErrSyncTimeout) {
				logger.Get().With(zap.Error(syncErr)).Warn("Replication sync timeout, forcing failover")
			} else {
				return nil, nil, syncErr
			}
		}
	}

	shard.Nodes[oldMasterNodeIndex].SetRole(RoleSlave)
	shard.Nodes[newMasterNodeIndex].SetRole(RoleMaster)
	return oldMaster, newMaster, nil
}

func (shard *Shard) HasOverlap(slotRange SlotRange) bool {
	for _, shardSlotRange := range shard.SlotRanges {
		if shardSlotRange.HasOverlap(slotRange) {
			return true
		}
	}
	return false
}

// validateAddr rejects a blank/half-formed node address (e.g. ":6666" from an unresolved hostname).
// Such an address serializes into a malformed CLUSTERX SETNODES line and registers a phantom,
// unreachable node, so every boundary that accepts an address must fail loudly on one.
func validateAddr(addr string) error {
	if host, port, err := net.SplitHostPort(addr); err != nil || host == "" || port == "" {
		return fmt.Errorf("%w: node address must be host:port, got %q", consts.ErrInvalidArgument, addr)
	}
	return nil
}

func (shard *Shard) ToSlotsString() (string, error) {
	var builder strings.Builder
	masterNodeIndex := -1
	for i, node := range shard.Nodes {
		if node.IsMaster() {
			masterNodeIndex = i
			break
		}
	}
	if masterNodeIndex == -1 {
		return "", errors.New("missing master node")
	}

	for i, node := range shard.Nodes {
		if err := validateAddr(node.Addr()); err != nil {
			return "", fmt.Errorf("node %s: %w", node.ID(), err)
		}
		builder.WriteString(node.ID())
		builder.WriteByte(' ')
		builder.WriteString(strings.Replace(node.Addr(), ":", " ", 1))
		builder.WriteByte(' ')
		if i == masterNodeIndex {
			builder.WriteString(RoleMaster)
			builder.WriteByte(' ')
			builder.WriteByte('-')
			builder.WriteByte(' ')
			for j, slotRange := range shard.SlotRanges {
				builder.WriteString(slotRange.String())
				if j != len(shard.SlotRanges)-1 {
					builder.WriteByte(' ')
				}
			}
		} else {
			if node.Failed() {
				builder.WriteString(RoleSlave + ",fail")
			} else {
				builder.WriteString(RoleSlave)
			}
			builder.WriteByte(' ')
			builder.WriteString(shard.Nodes[masterNodeIndex].ID())
		}
		builder.WriteByte('\n')
	}
	return builder.String(), nil
}

// UnmarshalJSON unmarshal a Shard from JSON bytes,
// it's required since Shard.Nodes is an interface slice.
// So we need to take into a concrete type.
func (shard *Shard) UnmarshalJSON(bytes []byte) error {
	var data struct {
		SlotRanges       []SlotRange    `json:"slot_ranges"`
		TargetShardIndex int            `json:"target_shard_index"`
		MigratingSlot    *MigratingSlot `json:"migrating_slot"`
		Nodes            []*ClusterNode `json:"nodes"`
	}
	if err := json.Unmarshal(bytes, &data); err != nil {
		return err
	}
	shard.SlotRanges = data.SlotRanges
	shard.TargetShardIndex = data.TargetShardIndex
	shard.MigratingSlot = data.MigratingSlot
	shard.Nodes = make([]Node, len(data.Nodes))
	for i, node := range data.Nodes {
		shard.Nodes[i] = node
	}
	return nil
}
