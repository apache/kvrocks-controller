package storage

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/util"
)

// ListNodes return the list of nodes under the specified shard
func (s *Storage) ListNodes(ns, cluster string, shardIdx int) ([]metadata.NodeInfo, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()
	if !s.isLeaderAndReady() {
		return nil, ErrNoLeaderOrNotReady
	}
	shard, err := s.getShard(ns, cluster, shardIdx)
	if err != nil {
		return nil, fmt.Errorf("get shard: %w", err)
	}
	return shard.Nodes, nil
}

// GetMasterNode return the master of node under the specified shard
func (s *Storage) GetMasterNode(ns, cluster string, shardIdx int) (metadata.NodeInfo, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()
	if !s.isLeaderAndReady() {
		return metadata.NodeInfo{}, ErrNoLeaderOrNotReady
	}
	nodes, err := s.ListNodes(ns, cluster, shardIdx)
	if err != nil {
		return metadata.NodeInfo{}, err
	}

	for _, node := range nodes {
		if node.Role == metadata.RoleMaster {
			return node, nil
		}
	}
	return metadata.NodeInfo{}, metadata.ErrNodeNoExists
}

// CreateNode add a node under the specified shard
func (s *Storage) CreateNode(ns, cluster string, shardIdx int, node *metadata.NodeInfo) error {
	s.rw.Lock()
	defer s.rw.Unlock()
	if !s.isLeaderAndReady() {
		return ErrNoLeaderOrNotReady
	}
	clusterInfo, err := s.local.GetClusterCopy(ns, cluster)
	if err != nil {
		return fmt.Errorf("get cluster: %w", err)
	}
	if shardIdx >= len(clusterInfo.Shards) || shardIdx < 0 {
		return metadata.ErrShardIndexOutOfRange
	}
	shard := clusterInfo.Shards[shardIdx]
	if shard.Nodes == nil {
		shard.Nodes = make([]metadata.NodeInfo, 0)
	}
	for _, existedNode := range shard.Nodes {
		if existedNode.Address == node.Address {
			return metadata.NewError("existedNode", metadata.CodeExisted, "")
		}
	}
	// NodeRole check
	if len(shard.Nodes) == 0 && !node.IsMaster() {
		return errors.New("you MUST add master node first")
	}
	if len(shard.Nodes) != 0 && node.IsMaster() {
		return errors.New("the master node has already added in this shard")
	}
	clusterInfo.Version++
	clusterInfo.Shards[shardIdx].Nodes = append(clusterInfo.Shards[shardIdx].Nodes, *node)
	if err := s.updateCluster(ns, cluster, &clusterInfo); err != nil {
		return err
	}
	s.EmitEvent(Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     shardIdx,
		NodeID:    node.ID,
		Type:      EventNode,
		Command:   CommandCreate,
	})
	return nil
}

// RemoveSlaveNode delete the node from the specified shard
func (s *Storage) RemoveSlaveNode(ns, cluster string, shardIdx int, nodeID string) error {
	s.rw.Lock()
	defer s.rw.Unlock()
	if !s.isLeaderAndReady() {
		return ErrNoLeaderOrNotReady
	}
	if len(nodeID) < metadata.NodeIdMinLen {
		return errors.New("nodeid len to short")
	}
	clusterInfo, err := s.local.GetClusterCopy(ns, cluster)
	if err != nil {
		return fmt.Errorf("get cluster: %w", err)
	}
	if shardIdx >= len(clusterInfo.Shards) || shardIdx < 0 {
		return metadata.ErrShardIndexOutOfRange
	}
	shard := clusterInfo.Shards[shardIdx]
	if shard.Nodes == nil {
		return metadata.NewError("node", metadata.CodeNoExists, "")
	}
	nodeIdx := -1
	for idx, node := range shard.Nodes {
		if strings.HasPrefix(node.ID, nodeID) {
			nodeIdx = idx
			break
		}
	}
	if nodeIdx == -1 {
		return metadata.NewError("node", metadata.CodeNoExists, "")
	}
	node := shard.Nodes[nodeIdx]
	if len(shard.SlotRanges) != 0 {
		if len(shard.Nodes) == 1 || node.IsMaster() {
			return errors.New("still some slots in this shard, please migrate them first")
		}
	} else {
		if node.IsMaster() && len(shard.Nodes) > 1 {
			return errors.New("please remove slave Shards first")
		}
	}
	clusterInfo.Version++
	clusterInfo.Shards[shardIdx].Nodes = append(clusterInfo.Shards[shardIdx].Nodes[:nodeIdx], clusterInfo.Shards[shardIdx].Nodes[nodeIdx+1:]...)
	if err := s.updateCluster(ns, cluster, &clusterInfo); err != nil {
		return err
	}
	s.EmitEvent(Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     shardIdx,
		NodeID:    node.ID,
		Type:      EventNode,
		Command:   CommandRemove,
	})
	return nil
}

// RemoveMasterNode delete the master node from the specified shard
func (s *Storage) RemoveMasterNode(ns, cluster string, shardIdx int, nodeID string) error {
	s.rw.Lock()
	defer s.rw.Unlock()
	if !s.isLeaderAndReady() {
		return ErrNoLeaderOrNotReady
	}

	clusterInfo, err := s.local.GetClusterCopy(ns, cluster)
	if err != nil {
		return fmt.Errorf("get cluster: %w", err)
	}
	if shardIdx >= len(clusterInfo.Shards) || shardIdx < 0 {
		return metadata.ErrShardIndexOutOfRange
	}
	shard := clusterInfo.Shards[shardIdx]
	if shard.Nodes == nil {
		return metadata.NewError("node", metadata.CodeNoExists, "")
	}
	var (
		nodeIdx   = -1
		targetIdx = -1
		offset    uint64
	)
	for idx, node := range shard.Nodes {
		if strings.HasPrefix(node.ID, nodeID) {
			nodeIdx = idx
		} else {
			nodeInfo, err := util.NodeInfoCmd(node.Address)
			if err != nil {
				continue
			}
			offsetNodeStr := nodeInfo.SlaveReplication.SlaveReplOffset
			if len(offsetNodeStr) == 0 {
				continue
			}
			offsetNode, _ := strconv.ParseUint(offsetNodeStr, 10, 64)
			if offsetNode > offset {
				offset = offsetNode
				targetIdx = idx
			}
		}
	}
	if nodeIdx == -1 {
		return metadata.NewError("node", metadata.CodeNoExists, "no master")
	}
	if targetIdx == -1 {
		return metadata.NewError("node", metadata.CodeNoExists, "no slave to switch")
	}

	targetNode := clusterInfo.Shards[shardIdx].Nodes[targetIdx]
	targetNode.Role = metadata.RoleMaster
	shard.Nodes = []metadata.NodeInfo{targetNode}
	for _, node := range clusterInfo.Shards[shardIdx].Nodes {
		if strings.HasPrefix(node.ID, nodeID) {
			continue
		}
		if targetNode.ID == node.ID {
			continue
		}
		shard.Nodes = append(shard.Nodes, node)
	}
	clusterInfo.Shards[shardIdx] = shard
	clusterInfo.Version++
	clusterInfo.Shards[shardIdx] = shard
	if err := s.updateCluster(ns, cluster, &clusterInfo); err != nil {
		return err
	}
	s.EmitEvent(Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     shardIdx,
		NodeID:    targetNode.ID,
		Type:      EventNode,
		Command:   CommandRemove,
	})
	return nil
}

// UpdateNode update exists node under the specified shard
func (s *Storage) UpdateNode(ns, cluster string, shardIdx int, node *metadata.NodeInfo) error {
	s.rw.Lock()
	defer s.rw.Unlock()
	if !s.isLeaderAndReady() {
		return ErrNoLeaderOrNotReady
	}
	clusterInfo, err := s.local.GetClusterCopy(ns, cluster)
	if err != nil {
		return fmt.Errorf("get cluster: %w", err)
	}
	if shardIdx >= len(clusterInfo.Shards) || shardIdx < 0 {
		return metadata.ErrShardIndexOutOfRange
	}
	shard := clusterInfo.Shards[shardIdx]
	if shard.Nodes == nil {
		return metadata.ErrNodeNoExists
	}
	// TODO: check the role
	for idx, existedNode := range shard.Nodes {
		if existedNode.ID == node.ID {
			clusterInfo.Version++
			clusterInfo.Shards[shardIdx].Nodes[idx] = *node
			if err := s.updateCluster(ns, cluster, &clusterInfo); err != nil {
				return err
			}
			s.EmitEvent(Event{
				Namespace: ns,
				Cluster:   cluster,
				Shard:     shardIdx,
				NodeID:    node.ID,
				Type:      EventNode,
				Command:   CommandUpdate,
			})
			return nil
		}
	}
	return metadata.NewError("node", metadata.CodeNoExists, "")
}
