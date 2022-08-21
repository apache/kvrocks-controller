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
func (stor *Storage) ListNodes(ns, cluster string, shardIdx int) ([]metadata.NodeInfo, error) {
	stor.rw.RLock()
	defer stor.rw.RUnlock()
	if !stor.selfLeaderReady() {
		return nil, ErrSlaveNoSupport
	}
	shard, err := stor.getShard(ns, cluster, shardIdx)
	if err != nil {
		return nil, fmt.Errorf("get shard: %w", err)
	}
	return shard.Nodes, nil
}

// GetMasterNode return the master of node under the specified shard
func (stor *Storage) GetMasterNode(ns, cluster string, shardIdx int) (metadata.NodeInfo, error) {
	stor.rw.RLock()
	defer stor.rw.RUnlock()
	if !stor.selfLeaderReady() {
		return metadata.NodeInfo{}, ErrSlaveNoSupport
	}
	nodes, err := stor.ListNodes(ns, cluster, shardIdx)
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
func (stor *Storage) CreateNode(ns, cluster string, shardIdx int, node *metadata.NodeInfo) error {
	stor.rw.Lock()
	defer stor.rw.Unlock()
	if !stor.selfLeaderReady() {
		return ErrSlaveNoSupport
	}
	topo, err := stor.local.GetClusterCopy(ns, cluster)
	if err != nil {
		return fmt.Errorf("get cluster: %w", err)
	}
	if shardIdx >= len(topo.Shards) || shardIdx < 0 {
		return metadata.ErrShardIndexOutOfRange
	}
	shard := topo.Shards[shardIdx]
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
	topo.Version++
	topo.Shards[shardIdx].Nodes = append(topo.Shards[shardIdx].Nodes, *node)
	if err := stor.updateCluster(ns, cluster, &topo); err != nil {
		return err
	}
	stor.EmitEvent(Event{
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
func (stor *Storage) RemoveSlaveNode(ns, cluster string, shardIdx int, nodeID string) error {
	stor.rw.Lock()
	defer stor.rw.Unlock()
	if !stor.selfLeaderReady() {
		return ErrSlaveNoSupport
	}
	if len(nodeID) < metadata.NodeIdMinLen {
		return errors.New("nodeid len to short")
	}
	topo, err := stor.local.GetClusterCopy(ns, cluster)
	if err != nil {
		return fmt.Errorf("get cluster: %w", err)
	}
	if shardIdx >= len(topo.Shards) || shardIdx < 0 {
		return metadata.ErrShardIndexOutOfRange
	}
	shard := topo.Shards[shardIdx]
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
	topo.Version++
	topo.Shards[shardIdx].Nodes = append(topo.Shards[shardIdx].Nodes[:nodeIdx], topo.Shards[shardIdx].Nodes[nodeIdx+1:]...)
	if err := stor.updateCluster(ns, cluster, &topo); err != nil {
		return err
	}
	stor.EmitEvent(Event{
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
func (stor *Storage) RemoveMasterNode(ns, cluster string, shardIdx int, nodeID string) error {
	stor.rw.Lock()
	defer stor.rw.Unlock()
	if !stor.selfLeaderReady() {
		return ErrSlaveNoSupport
	}

	topo, err := stor.local.GetClusterCopy(ns, cluster)
	if err != nil {
		return fmt.Errorf("get cluster: %w", err)
	}
	if shardIdx >= len(topo.Shards) || shardIdx < 0 {
		return metadata.ErrShardIndexOutOfRange
	}
	shard := topo.Shards[shardIdx]
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

	targetNode := topo.Shards[shardIdx].Nodes[targetIdx]
	targetNode.Role = metadata.RoleMaster
	shard.Nodes = []metadata.NodeInfo{targetNode}
	for _, node := range topo.Shards[shardIdx].Nodes {
		if strings.HasPrefix(node.ID, nodeID) {
			continue
		}
		if targetNode.ID == node.ID {
			continue
		}
		shard.Nodes = append(shard.Nodes, node)
	}
	topo.Shards[shardIdx] = shard
	topo.Version++
	topo.Shards[shardIdx] = shard
	if err := stor.updateCluster(ns, cluster, &topo); err != nil {
		return err
	}
	stor.EmitEvent(Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     shardIdx,
		NodeID:    targetNode.ID,
		Type:      EventNode,
		Command:   CommandRemove,
	})
	return nil
}

// UpdateNode update the exist node under the specified shard
func (stor *Storage) UpdateNode(ns, cluster string, shardIdx int, node *metadata.NodeInfo) error {
	stor.rw.Lock()
	defer stor.rw.Unlock()
	if !stor.selfLeaderReady() {
		return ErrSlaveNoSupport
	}
	topo, err := stor.local.GetClusterCopy(ns, cluster)
	if err != nil {
		return fmt.Errorf("get cluster: %w", err)
	}
	if shardIdx >= len(topo.Shards) || shardIdx < 0 {
		return metadata.ErrShardIndexOutOfRange
	}
	shard := topo.Shards[shardIdx]
	if shard.Nodes == nil {
		return metadata.ErrNodeNoExists
	}
	// TODO: check the role
	for idx, existedNode := range shard.Nodes {
		if existedNode.ID == node.ID {
			topo.Version++
			topo.Shards[shardIdx].Nodes[idx] = *node
			if err := stor.updateCluster(ns, cluster, &topo); err != nil {
				return err
			}
			stor.EmitEvent(Event{
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
