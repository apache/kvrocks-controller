package metadata

import (
	"fmt"
	"strings"
)

type ClusterConfig struct {
  Name              string `json:"name"`
  HeartBeatInterval uint64 `json:"heartbeatinterval"`
  HeartBeatRetrys   uint64 `json:"heartbeatretrys"`
  
  MigrateConfig  interface{}   // TODO: migrate  submodel
  FailoverConfig interface{}   // TODO: failover submodel
}

type Cluster struct {
	Version int64          `json:"version"`
	Shards  []Shard        `json:"shards"`
	Config  ClusterConfig  `json:"config"`
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
