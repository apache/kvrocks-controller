package util

import (
	"strings"
	"context"
	"strconv"

	"github.com/go-redis/redis/v8"
)

type ClusterInfo struct {
	ClusterState         bool
	ClusterSlotsAssigned int
	ClusterSlotsOK       int
	ClusterSlotsPFail    int
	ClusterSlotsFail     int 
	ClusterKnownNodes    int
	ClusterSize          int 
	ClusterCurrentEpoch  uint64
	ClusterMyEpoch       uint64
	MigratingSlot        int 
	ImportingSlot        int
	DestinationNode      string
	MigratingState       string
	ImportingState       string
}

func ClusterInfoCmd(nodeAddr string) (*ClusterInfo, error) {
	cli := redis.NewClient(&redis.Options{
		Addr: nodeAddr,
	})
	res, err:= cli.Do(context.Background(), "CLUSTER", "info").Result()
	if err != nil {
		return nil, err
	}
	clusterInfo := &ClusterInfo{
		MigratingSlot: -1,
		ImportingSlot: -1,
		DestinationNode: "",
		MigratingState: "",
		ImportingState: "",
	}
	infos := strings.Split(res.(string), "\n")
	for _, info := range infos {
		kv := strings.Split(info, ":")
		if len(kv) != 2 {
			continue
		}
		key := strings.TrimSpace(kv[0])
		val := strings.TrimSpace(kv[1])
		switch key {
		case "cluster_state":
			if val == "ok" {
				clusterInfo.ClusterState = true
			} 
		case "cluster_slots_assigned":
			clusterInfo.ClusterSlotsAssigned, _ = strconv.Atoi(val)
		case "cluster_slots_ok":
			clusterInfo.ClusterSlotsOK, _ = strconv.Atoi(val)
		case "cluster_slots_pfail":
			clusterInfo.ClusterSlotsPFail, _ = strconv.Atoi(val)
		case "cluster_slots_fail":
			clusterInfo.ClusterSlotsFail, _ = strconv.Atoi(val)
		case "cluster_known_nodes":
			clusterInfo.ClusterKnownNodes, _ = strconv.Atoi(val)
		case "cluster_size":
			clusterInfo.ClusterSize, _ = strconv.Atoi(val)
		case "cluster_current_epoch":
			clusterInfo.ClusterCurrentEpoch, _ = strconv.ParseUint(val, 10, 64)
		case "cluster_my_epoch":
			clusterInfo.ClusterMyEpoch, _ = strconv.ParseUint(val, 10, 64)
		case "migrating_slot":
			clusterInfo.MigratingSlot, _ = strconv.Atoi(val)
		case "importing_slot":
			clusterInfo.ImportingSlot, _ = strconv.Atoi(val)
		case "destination_node":
			clusterInfo.DestinationNode = val
		case "migrating_state":
			clusterInfo.MigratingState = val
		case "import_state":
			clusterInfo.ImportingState = val
		}
	}
	return clusterInfo, nil
}