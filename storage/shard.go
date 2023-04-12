package storage

import (
	"errors"
	"fmt"

	"golang.org/x/net/context"

	"github.com/KvrocksLabs/kvrocks_controller/metadata"
)

func (s *Storage) ListShard(ctx context.Context, ns, cluster string) ([]metadata.Shard, error) {
	clusterInfo, err := s.GetClusterInfo(ctx, ns, cluster)
	if err != nil {
		return nil, err
	}
	return clusterInfo.Shards, nil
}

func (s *Storage) GetShard(ctx context.Context, ns, cluster string, shardIdx int) (*metadata.Shard, error) {
	return s.getShard(ctx, ns, cluster, shardIdx)
}

func (s *Storage) getShard(ctx context.Context, ns, cluster string, shardIdx int) (*metadata.Shard, error) {
	clusterInfo, err := s.GetClusterInfo(ctx, ns, cluster)
	if err != nil {
		return nil, err
	}
	if clusterInfo.Shards == nil {
		return nil, metadata.ErrEntryNoExists
	}
	if shardIdx >= len(clusterInfo.Shards) || shardIdx < 0 {
		return nil, metadata.ErrIndexOutOfRange
	}
	return &clusterInfo.Shards[shardIdx], nil
}

// CreateShard add a shard under the specified cluster
func (s *Storage) CreateShard(ctx context.Context, ns, cluster string, shard *metadata.Shard) error {
	clusterInfo, err := s.GetClusterInfo(ctx, ns, cluster)
	if err != nil {
		return err
	}
	clusterInfo.Version++
	clusterInfo.Shards = append(clusterInfo.Shards, *shard)
	if err := s.updateCluster(ctx, ns, clusterInfo); err != nil {
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
func (s *Storage) RemoveShard(ctx context.Context, ns, cluster string, shardIdx int) error {
	clusterInfo, err := s.GetClusterInfo(ctx, ns, cluster)
	if err != nil {
		return err
	}
	if shardIdx >= len(clusterInfo.Shards) || shardIdx < 0 {
		return metadata.ErrIndexOutOfRange
	}
	shard := clusterInfo.Shards[shardIdx]
	if len(shard.SlotRanges) > 0 {
		return fmt.Errorf("need to delete all slots before removing shard")
	}
	clusterInfo.Version++
	clusterInfo.Shards = append(clusterInfo.Shards[:shardIdx], clusterInfo.Shards[shardIdx+1:]...)
	if err := s.updateCluster(ctx, ns, clusterInfo); err != nil {
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
func (s *Storage) HasSlot(ctx context.Context, ns, cluster string, shardIdx, slot int) (bool, error) {
	shard, err := s.GetShard(ctx, ns, cluster, shardIdx)
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
func (s *Storage) AddShardSlots(ctx context.Context, ns, cluster string, shardIdx int, slotRanges []metadata.SlotRange) error {
	clusterInfo, err := s.GetClusterInfo(ctx, ns, cluster)
	if err != nil {
		return err
	}
	shard, err := s.getShard(ctx, ns, cluster, shardIdx)
	if err != nil {
		return fmt.Errorf("get shard: %w", err)
	}
	if len(shard.Nodes) == 0 {
		return errors.New("the shard was empty, please add Shards first")
	}
	clusterInfo.Version++
	clusterInfo.Shards[shardIdx].SlotRanges = metadata.MergeSlotRanges(shard.SlotRanges, slotRanges)
	if err := s.updateCluster(ctx, ns, clusterInfo); err != nil {
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

func (s *Storage) RemoveShardSlots(ctx context.Context, ns, cluster string, shardIdx int, slotRanges []metadata.SlotRange) error {
	clusterInfo, err := s.GetClusterInfo(ctx, ns, cluster)
	if err != nil {
		return err
	}
	shard, err := s.getShard(ctx, ns, cluster, shardIdx)
	if err != nil {
		return fmt.Errorf("get shard: %w", err)
	}
	clusterInfo.Version++
	clusterInfo.Shards[shardIdx].SlotRanges = metadata.RemoveSlotRanges(shard.SlotRanges, slotRanges)
	if err := s.updateCluster(ctx, ns, clusterInfo); err != nil {
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

func (s *Storage) UpdateMigrateSlotInfo(ctx context.Context, ns, cluster string, sourceIdx, targetIdx, slot int) error {
	clusterInfo, err := s.GetClusterInfo(ctx, ns, cluster)
	if err != nil {
		return err
	}
	if clusterInfo.Shards == nil {
		return metadata.ErrEntryNoExists
	}
	if sourceIdx >= len(clusterInfo.Shards) || sourceIdx < 0 {
		return metadata.ErrIndexOutOfRange
	}
	if targetIdx >= len(clusterInfo.Shards) || targetIdx < 0 {
		return metadata.ErrIndexOutOfRange
	}

	sourceShard := clusterInfo.Shards[sourceIdx]
	targetShard := clusterInfo.Shards[targetIdx]
	slotRanges := []metadata.SlotRange{{Start: slot, Stop: slot}}
	clusterInfo.Version++
	clusterInfo.Shards[sourceIdx].SlotRanges = metadata.RemoveSlotRanges(sourceShard.SlotRanges, slotRanges)
	clusterInfo.Shards[targetIdx].SlotRanges = metadata.MergeSlotRanges(targetShard.SlotRanges, slotRanges)
	if err := s.updateCluster(ctx, ns, clusterInfo); err != nil {
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
