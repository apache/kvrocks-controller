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
	clusterInfo, err := s.instance.GetClusterInfo(ns, cluster)
	if err != nil {
		return fmt.Errorf("get cluster: %w", err)
	}
	if shardIdx >= len(clusterInfo.Shards) || shardIdx < 0 {
		return metadata.ErrShardIndexOutOfRange
	}
	for _, shard := range clusterInfo.Shards {
		if len(shard.Nodes) == 0 {
			continue
		}
		for _, existedNode := range shard.Nodes {
			if existedNode.Address == node.Address || existedNode.ID == node.ID {
				return metadata.ErrNodeHasExisted
			}
		}
	}

	shard := clusterInfo.Shards[shardIdx]
	if shard.Nodes == nil {
		shard.Nodes = make([]metadata.NodeInfo, 0)
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

// RemoveNode delete the node from the specified shard
func (s *Storage) RemoveNode(ns, cluster string, shardIdx int, nodeID string) error {
	s.rw.Lock()
	defer s.rw.Unlock()
	if len(nodeID) != metadata.NodeIdMinLen {
		return errors.New("invalid node length")
	}
	clusterInfo, err := s.instance.GetClusterInfo(ns, cluster)
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
	nodeIdx := -1
	for idx, node := range shard.Nodes {
		if strings.HasPrefix(node.ID, nodeID) {
			nodeIdx = idx
			break
		}
	}
	if nodeIdx == -1 {
		return metadata.ErrClusterNoExists
	}
	node := shard.Nodes[nodeIdx]
	if len(shard.SlotRanges) != 0 {
		if len(shard.Nodes) == 1 || node.IsMaster() {
			return errors.New("still some slots in this shard, please migrate them first")
		}
	} else {
		if node.IsMaster() && len(shard.Nodes) > 1 {
			return errors.New("please remove slave shards first")
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

// PromoteNewMaster delete the master node from the specified shard
func (s *Storage) PromoteNewMaster(ns, cluster string, shardIdx int, oldMasterNodeID string) error {
	s.rw.Lock()
	defer s.rw.Unlock()

	clusterInfo, err := s.instance.GetClusterInfo(ns, cluster)
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
	var (
		oldMasterNodeIndex = -1
		fastestSlaveIndex  = -1
		maxReplOffset      uint64
	)
	for idx, node := range shard.Nodes {
		if node.ID == oldMasterNodeID {
			oldMasterNodeIndex = idx
			continue
		}
		nodeInfo, err := util.NodeInfoCmd(node.Address)
		if err != nil {
			continue
		}
		offsetNodeStr := nodeInfo.SlaveReplication.SlaveReplOffset
		if len(offsetNodeStr) == 0 {
			continue
		}
		offsetNode, _ := strconv.ParseUint(offsetNodeStr, 10, 64)
		if offsetNode >= maxReplOffset {
			maxReplOffset = offsetNode
			fastestSlaveIndex = idx
		}
	}
	if oldMasterNodeIndex == -1 {
		return metadata.NewError("node", metadata.CodeNoExists, "current master node is not exists")
	}
	if fastestSlaveIndex == -1 {
		return metadata.NewError("node", metadata.CodeNoExists, "no candidate to be promoted")
	}

	shard.Nodes[fastestSlaveIndex].Role = metadata.RoleMaster
	shard.Nodes[oldMasterNodeIndex].Role = metadata.RoleSlave
	clusterInfo.Version++
	clusterInfo.Shards[shardIdx] = shard
	if err := s.updateCluster(ns, cluster, &clusterInfo); err != nil {
		return err
	}
	s.EmitEvent(Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     shardIdx,
		NodeID:    shard.Nodes[fastestSlaveIndex].ID,
		Type:      EventNode,
		Command:   CommandRemove,
	})
	return nil
}

// UpdateNode update exists node under the specified shard
func (s *Storage) UpdateNode(ns, cluster string, shardIdx int, node *metadata.NodeInfo) error {
	s.rw.Lock()
	defer s.rw.Unlock()

	clusterInfo, err := s.instance.GetClusterInfo(ns, cluster)
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
