package metadata

import (
	"fmt"
	"strings"
	"errors"
	"sort"
	"strconv"
)

type ClusterConfig struct {
	Name              string `json:"name"`
	HeartBeatInterval uint64 `json:"heartbeat_interval"`
	HeartBeatRetries  uint64 `json:"heartbeat_retries"`

	MigrateConfig  interface{}
	FailoverConfig interface{}
}

type Cluster struct {
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

func ParserToCluster(clusterStr string) (*Cluster, error) {
	if len(clusterStr) == 0 {
		return nil, errors.New("cluster nodes string error")
	}
	nodeStrs := strings.Split(clusterStr, "\n")
	if len(nodeStrs) == 0 {
		return nil, errors.New("cluster nodes string parser error")
	}

	var clusterVer int64 = -1
	var shards Shards
	slaveNodes := make(map[string][]NodeInfo) 
	for _, nodestr := range nodeStrs {
		nodeEle := strings.Split(nodestr, " ")
		if len(nodeEle) < 7 {
			return nil, fmt.Errorf("node elements less 7, node info[%q]", nodestr)
		}
		node := NodeInfo{
			ID:      nodeEle[0],
			Address: strings.Split(nodeEle[1], "@")[0],
		}

		if strings.Contains(nodeEle[2], ",") {
			node.Role = strings.Split(nodeEle[2], ",")[1]
			var err error
			clusterVer, err = strconv.ParseInt(nodeEle[6], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("node version error, node info[%q]", nodestr)
			}
		} else {
			node.Role = nodeEle[2]
		}

		if node.Role == RoleMaster {
			if len(nodeEle) < 9 {
				return nil, fmt.Errorf("master node element less 9, node info[%q]", nodestr)
			}
			slots, err := ParseSlotRange(nodeEle[8])
			if err != nil {
				return nil, fmt.Errorf("master node parser slot error, node info[%q]", nodestr)
			}
			shard := Shard{}
			shard.Nodes = append(shard.Nodes, node)
			shard.SlotRanges = append(shard.SlotRanges, *slots)
			shards = append(shards, shard)
		} else if node.Role == RoleSlave {
			slaveNodes[nodeEle[3]] = append(slaveNodes[nodeEle[3]], node)
		} else {
			return nil, fmt.Errorf("node role error, node info[%q]", nodestr)
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
		Shards: shards,
	}, nil
}
