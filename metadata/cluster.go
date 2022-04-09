package metadata

import (
	"fmt"
	"strings"
)

type Cluster struct {
	Version int     `json:"version"`
	Shards  []Shard `json:"shards"`
}

func (cluster *Cluster) CheckOverlap(slotRange *SlotRange) error {
	for idx, shard := range cluster.Shards {
		if shard.HasOverlap(slotRange) {
			return fmt.Errorf("the slot range was owned by shard: %d", idx)
		}
	}
	return nil
}

func (cluster *Cluster) ToSlotString() (string, error) {
	var builder strings.Builder
	for i, shard := range cluster.Shards {
		shardSlotsString, err := shard.ToSlotsString()
		if err != nil {
			return "", fmt.Errorf("found err at shard[%d]: %w", i, err)
		}
		builder.WriteString(shardSlotsString)
	}
	return builder.String(), nil
}
