package storage

import (
	"errors"
	"fmt"

	"github.com/KvrocksLabs/kvrocks_controller/metadata"
)

// ListShard return the list of name of Shard under the specified cluster
func (s *Storage) ListShard(ns, cluster string) ([]metadata.Shard, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()
	if !s.isLeaderAndReady() {
		return nil, ErrNoLeaderOrNotReady
	}
	topo, err := s.instance.GetClusterInfo(ns, cluster)
	if err != nil {
		return nil, err
	}
	return topo.Shards, nil
}

// GetShard return the shard under the specified cluster
func (s *Storage) GetShard(ns, cluster string, shardIdx int) (*metadata.Shard, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()
	if !s.isLeaderAndReady() {
		return nil, ErrNoLeaderOrNotReady
	}
	return s.getShard(ns, cluster, shardIdx)
}

func (s *Storage) getShard(ns, cluster string, shardIdx int) (*metadata.Shard, error) {
	clusterInfo, err := s.instance.GetClusterInfo(ns, cluster)
	if err != nil {
		return nil, err
	}
	if clusterInfo.Shards == nil {
		return nil, metadata.ErrShardNoExists
	}
	if shardIdx >= len(clusterInfo.Shards) || shardIdx < 0 {
		return nil, metadata.ErrShardIndexOutOfRange
	}
	return &clusterInfo.Shards[shardIdx], nil
}

// CreateShard add a shard under the specified cluster
func (s *Storage) CreateShard(ns, cluster string, shard *metadata.Shard) error {
	s.rw.Lock()
	defer s.rw.Unlock()
	if !s.isLeaderAndReady() {
		return ErrNoLeaderOrNotReady
	}
	clusterInfo, err := s.instance.GetClusterInfo(ns, cluster)
	if err != nil {
		return err
	}
	clusterInfo.Version++
	clusterInfo.Shards = append(clusterInfo.Shards, *shard)
	if err := s.updateCluster(ns, cluster, &clusterInfo); err != nil {
		return err
	}
	s.EmitEvent(Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     len(clusterInfo.Shards) - 1,
		Type:      EventShard,
		Command:   CommandCreate,
	})
	return nil
}

// RemoveShard delete the shard under the specified cluster
func (s *Storage) RemoveShard(ns, cluster string, shardIdx int) error {
	s.rw.Lock()
	defer s.rw.Unlock()
	if !s.isLeaderAndReady() {
		return ErrNoLeaderOrNotReady
	}
	clusterInfo, err := s.instance.GetClusterInfo(ns, cluster)
	if err != nil {
		return err
	}
	if shardIdx >= len(clusterInfo.Shards) || shardIdx < 0 {
		return metadata.ErrShardIndexOutOfRange
	}
	shard := clusterInfo.Shards[shardIdx]
	if len(shard.SlotRanges) > 0 {
		return fmt.Errorf("need to delete all slots before removing shard")
	}
	clusterInfo.Version++
	clusterInfo.Shards = append(clusterInfo.Shards[:shardIdx], clusterInfo.Shards[shardIdx+1:]...)
	if err := s.updateCluster(ns, cluster, &clusterInfo); err != nil {
		return err
	}
	s.EmitEvent(Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     shardIdx,
		Type:      EventShard,
		Command:   CommandRemove,
	})
	return nil
}

// HasSlot return an indicator whether the slot under the specified Shard
func (s *Storage) HasSlot(ns, cluster string, shardIdx, slot int) (bool, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()
	if !s.isLeaderAndReady() {
		return false, ErrNoLeaderOrNotReady
	}
	shard, err := s.GetShard(ns, cluster, shardIdx)
	if err != nil {
		return false, err
	}
	for _, slots := range shard.SlotRanges {
		if slot >= slots.Start && slot <= slots.Stop {
			return true, nil
		}
	}
	return false, nil
}

// AddShardSlots add slotRanges to the specified shard under the specified cluster
func (s *Storage) AddShardSlots(ns, cluster string, shardIdx int, slotRanges []metadata.SlotRange) error {
	s.rw.Lock()
	defer s.rw.Unlock()
	if !s.isLeaderAndReady() {
		return ErrNoLeaderOrNotReady
	}
	clusterInfo, err := s.instance.GetClusterInfo(ns, cluster)
	if err != nil {
		return err
	}
	shard, err := s.getShard(ns, cluster, shardIdx)
	if err != nil {
		return fmt.Errorf("get shard: %w", err)
	}
	if len(shard.Nodes) == 0 {
		return errors.New("the shard was empty, please add Shards first")
	}
	clusterInfo.Version++
	clusterInfo.Shards[shardIdx].SlotRanges = metadata.MergeSlotRanges(shard.SlotRanges, slotRanges)
	if err := s.updateCluster(ns, cluster, &clusterInfo); err != nil {
		return err
	}
	s.EmitEvent(Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     shardIdx,
		Type:      EventShard,
		Command:   CommandAddSlots,
	})
	return nil
}

func (s *Storage) RemoveShardSlots(ns, cluster string, shardIdx int, slotRanges []metadata.SlotRange) error {
	s.rw.Lock()
	defer s.rw.Unlock()
	if !s.isLeaderAndReady() {
		return ErrNoLeaderOrNotReady
	}
	clusterInfo, err := s.instance.GetClusterInfo(ns, cluster)
	if err != nil {
		return err
	}
	shard, err := s.getShard(ns, cluster, shardIdx)
	if err != nil {
		return fmt.Errorf("get shard: %w", err)
	}
	clusterInfo.Version++
	clusterInfo.Shards[shardIdx].SlotRanges = metadata.RemoveSlotRanges(shard.SlotRanges, slotRanges)
	if err := s.updateCluster(ns, cluster, &clusterInfo); err != nil {
		return err
	}
	s.EmitEvent(Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     shardIdx,
		Type:      EventShard,
		Command:   CommandRemoveSlots,
	})
	return nil
}

// MigrateSlot delete slot from sourceIdx, and add slot to targetIdx
func (s *Storage) MigrateSlot(ns, cluster string, sourceIdx, targetIdx, slot int) error {
	s.rw.Lock()
	defer s.rw.Unlock()
	if !s.isLeaderAndReady() {
		return ErrNoLeaderOrNotReady
	}
	clusterInfo, err := s.instance.GetClusterInfo(ns, cluster)
	if err != nil {
		return err
	}
	if clusterInfo.Shards == nil {
		return metadata.ErrShardNoExists
	}
	if sourceIdx >= len(clusterInfo.Shards) || sourceIdx < 0 {
		return metadata.ErrShardIndexOutOfRange
	}
	if targetIdx >= len(clusterInfo.Shards) || targetIdx < 0 {
		return metadata.ErrShardIndexOutOfRange
	}
	// assume slot has been check that among sourceShard
	sourceShard := clusterInfo.Shards[sourceIdx]
	targetShard := clusterInfo.Shards[targetIdx]
	slotRanges := []metadata.SlotRange{{Start: slot, Stop: slot}}
	clusterInfo.Version++
	clusterInfo.Shards[sourceIdx].SlotRanges = metadata.RemoveSlotRanges(sourceShard.SlotRanges, slotRanges)
	clusterInfo.Shards[targetIdx].SlotRanges = metadata.MergeSlotRanges(targetShard.SlotRanges, slotRanges)
	if err := s.updateCluster(ns, cluster, &clusterInfo); err != nil {
		return err
	}
	s.EmitEvent(Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     sourceIdx,
		Type:      EventShard,
		Command:   CommandMigrateSlots,
	})
	return nil
}
