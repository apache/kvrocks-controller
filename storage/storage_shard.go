package storage

import (
	"fmt"
	"errors"

	"github.com/KvrocksLabs/kvrocks-controller/metadata"
)

// ListShard return the list of name of Shard under the specified cluster
func (stor *Storage) ListShard(ns, cluster string) ([]metadata.Shard, error) {
	stor.rw.RLock()
	defer stor.rw.RUnlock()
	if !stor.selfLeaderReady() {
		return nil, ErrSlaveNoSupport
	}
	topo, err := stor.local.GetClusterCopy(ns, cluster)
	if err != nil {
		return nil, err
	}
	return topo.Shards, nil
}

// GetShard retun the shard under the specified cluster
func (stor *Storage) GetShard(ns, cluster string, shardIdx int) (*metadata.Shard, error) {
	stor.rw.RLock()
	defer stor.rw.RUnlock()
	if !stor.selfLeaderReady() {
		return nil, ErrSlaveNoSupport
	}
	return stor.getShard(ns, cluster, shardIdx)
}

// getShard is goroutine unsafety of GetShard
// assumption caller has hold the lock
func (stor *Storage) getShard(ns, cluster string, shardIdx int) (*metadata.Shard, error) {
	topo, err := stor.local.GetClusterCopy(ns, cluster)
	if err != nil {
		return nil, err
	}
	if topo.Shards == nil {
		return nil, metadata.NewError("shard", metadata.CodeNoExists, "")
	}
	if shardIdx >= len(topo.Shards) || shardIdx < 0 {
		return nil, metadata.ErrShardIndexOutOfRange
	}
	return &topo.Shards[shardIdx], nil
}

// CreateShard add a shard under the specified cluster
func (stor *Storage) CreateShard(ns, cluster string, shard *metadata.Shard) error {
	stor.rw.Lock()
	defer stor.rw.Unlock()
	if !stor.selfLeaderReady() {
		return ErrSlaveNoSupport
	}
	topo, err := stor.local.GetClusterCopy(ns, cluster)
	if err != nil {
		return err
	}
	topo.Version++
	topo.Shards = append(topo.Shards, *shard)
	if err := stor.updateCluster(ns, cluster, &topo); err != nil {
		return err
	}
	stor.EmitEvent(Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     len(topo.Shards) - 1,
		Type:      EventShard,
		Command:   CommandCreate,
	})
	return nil
}

// RemoveShard delete the shard under the specified cluster
func (stor *Storage) RemoveShard(ns, cluster string, shardIdx int) error {
	stor.rw.Lock()
	defer stor.rw.Unlock()
	if !stor.selfLeaderReady() {
		return ErrSlaveNoSupport
	}
	topo, err := stor.local.GetClusterCopy(ns, cluster)
	if err != nil {
		return err
	}
	if shardIdx >= len(topo.Shards) || shardIdx < 0 {
		return metadata.ErrShardIndexOutOfRange
	}
	shard := topo.Shards[shardIdx]
	if len(shard.SlotRanges) > 0 {
		return fmt.Errorf("need to delete all slots before removing shard")
	}
	topo.Version++
	topo.Shards = append(topo.Shards[:shardIdx], topo.Shards[shardIdx+1:]...)
	if err := stor.updateCluster(ns, cluster, &topo); err != nil {
		return err
	}
	stor.EmitEvent(Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     shardIdx,
		Type:      EventShard,
		Command:   CommandRemove,
	})
	return nil
}

// HasSlot return an indicator whether the slot under the specified Shard
func (stor *Storage) HasSlot(ns, cluster string, shardIdx, slot int) (bool, error) {
	stor.rw.RLock()
	defer stor.rw.RUnlock()
	if !stor.selfLeaderReady() {
		return false, ErrSlaveNoSupport
	}
	shard, err:= stor.GetShard(ns, cluster, shardIdx)
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
func (stor *Storage) AddShardSlots(ns, cluster string, shardIdx int, slotRanges []metadata.SlotRange) error {
	stor.rw.Lock()
	defer stor.rw.Unlock()
	if !stor.selfLeaderReady() {
		return ErrSlaveNoSupport
	}
	topo, err := stor.local.GetClusterCopy(ns, cluster)
	if err != nil {
		return err
	}
	shard, err := stor.getShard(ns, cluster, shardIdx)
	if err != nil {
		return fmt.Errorf("get shard: %w", err)
	}
	if len(shard.Nodes) == 0 {
		return errors.New("the shard was empty, please add Shards first")
	}
	topo.Version++
	topo.Shards[shardIdx].SlotRanges = metadata.MergeSlotRanges(shard.SlotRanges, slotRanges)
	if err := stor.updateCluster(ns, cluster, &topo); err != nil {
		return err
	}
	stor.EmitEvent(Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     shardIdx,
		Type:      EventShard,
		Command:   CommandAddSlots,
	})
	return nil
}

// AddShardSlots delete slotRanges from the specified shard under the specified cluster
func (stor *Storage) RemoveShardSlots(ns, cluster string, shardIdx int, slotRanges []metadata.SlotRange) error {
	stor.rw.Lock()
	defer stor.rw.Unlock()
	if !stor.selfLeaderReady() {
		return ErrSlaveNoSupport
	}
	topo, err := stor.local.GetClusterCopy(ns, cluster)
	if err != nil {
		return err
	}
	shard, err := stor.getShard(ns, cluster, shardIdx)
	if err != nil {
		return fmt.Errorf("get shard: %w", err)
	}
	topo.Version++
	topo.Shards[shardIdx].SlotRanges = metadata.RemoveSlotRanges(shard.SlotRanges, slotRanges)
	if err := stor.updateCluster(ns, cluster, &topo); err != nil {
		return err
	}
	stor.EmitEvent(Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     shardIdx,
		Type:      EventShard,
		Command:   CommandRemoveSlots,
	})
	return nil
}

// MigrateSlot delete slot from sourceIdx, and add slot to targetIdx
func (stor *Storage) MigrateSlot(ns, cluster string, sourceIdx, targetIdx, slot int) error {
	stor.rw.Lock()
	defer stor.rw.Unlock()
	if !stor.selfLeaderReady() {
		return ErrSlaveNoSupport
	}
	topo, err := stor.local.GetClusterCopy(ns, cluster)
	if err != nil {
		return err
	}
	if topo.Shards == nil {
		return metadata.NewError("shard", metadata.CodeNoExists, "")
	}
	if sourceIdx >= len(topo.Shards) || sourceIdx < 0 {
		return metadata.ErrShardIndexOutOfRange
	}
	if targetIdx >= len(topo.Shards) || targetIdx < 0 {
		return metadata.ErrShardIndexOutOfRange
	}
	// assume slot has been check that among sourceShard
	sourceShard := topo.Shards[sourceIdx]
	targetShard := topo.Shards[targetIdx]
	slotRanges := []metadata.SlotRange{metadata.SlotRange{Start: slot, Stop:slot}}
	topo.Version++
	topo.Shards[sourceIdx].SlotRanges = metadata.RemoveSlotRanges(sourceShard.SlotRanges, slotRanges)
	topo.Shards[targetIdx].SlotRanges = metadata.MergeSlotRanges(targetShard.SlotRanges, slotRanges)
	if err := stor.updateCluster(ns, cluster, &topo); err != nil {
		return err
	}
	stor.EmitEvent(Event{
		Namespace: ns,
		Cluster:   cluster,
		Shard:     sourceIdx,
		Type:      EventShard,
		Command:   CommandMigrateSlots,
	})
	return nil
}
