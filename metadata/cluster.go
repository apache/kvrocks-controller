package metadata

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type ClusterConfig struct {
	Name              string `json:"name"`
	HeartBeatInterval uint64 `json:"heartbeat_interval"`
	HeartBeatRetries  uint64 `json:"heartbeat_retries"`
}

type Cluster struct {
	Name    string        `json:"name"`
	Version int64         `json:"version"`
	Shards  []Shard       `json:"shards"`
	Config  ClusterConfig `json:"config"`
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

// ParseCluster will parse the cluster string into cluster topology.
func ParseCluster(clusterStr string) (*Cluster, error) {
	if len(clusterStr) == 0 {
		return nil, errors.New("cluster nodes string error")
	}
	nodeStrings := strings.Split(clusterStr, "\n")
	if len(nodeStrings) == 0 {
		return nil, errors.New("cluster nodes string parser error")
	}

	var clusterVer int64 = -1
	var shards Shards
	slaveNodes := make(map[string][]NodeInfo)
	for _, nodeString := range nodeStrings {
		fields := strings.Split(nodeString, " ")
		if len(fields) < 7 {
			return nil, fmt.Errorf("require at least 7 fields, node info[%s]", nodeString)
		}
		node := NodeInfo{
			ID:      fields[0],
			Address: strings.Split(fields[1], "@")[0],
		}

		if strings.Contains(fields[2], ",") {
			node.Role = strings.Split(fields[2], ",")[1]
			var err error
			clusterVer, err = strconv.ParseInt(fields[6], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("node version error, node info[%q]", nodeString)
			}
		} else {
			node.Role = fields[2]
		}

		if node.Role == RoleMaster {
			if len(fields) < 9 {
				return nil, fmt.Errorf("master node element less 9, node info[%q]", nodeString)
			}
			slots, err := ParseSlotRange(fields[8])
			if err != nil {
				return nil, fmt.Errorf("master node parser slot error, node info[%q]", nodeString)
			}
			shard := Shard{}
			shard.Nodes = append(shard.Nodes, node)
			shard.SlotRanges = append(shard.SlotRanges, *slots)
			shards = append(shards, shard)
		} else if node.Role == RoleSlave {
			slaveNodes[fields[3]] = append(slaveNodes[fields[3]], node)
		} else {
			return nil, fmt.Errorf("node role error, node info[%q]", nodeString)
		}
	}
	if clusterVer == -1 {
		return nil, fmt.Errorf("no cluster version, cluster info[%q]", clusterStr)
	}
	sort.Sort(shards)
	for i := 0; i < len(shards); i++ {
		masterNode := shards[i].Nodes[0]
		shards[i].Nodes = append(shards[i].Nodes, slaveNodes[masterNode.ID]...)
	}
	return &Cluster{
		Version: clusterVer,
		Shards:  shards,
	}, nil
}
