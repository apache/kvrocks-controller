package handlers

import (
	"strconv"

	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/storage/base/etcd"
)

type CreateNamespaceParam struct {
	Namespace string `json:namespace`
}

type CreateClusterParam struct {
	Cluster string             `json:"cluster"`
	Shards  []CreateShardParam `json:"shards"`
}

type CreateShardParam struct {
	Master *metadata.NodeInfo  `json:"master"`
	Slaves []metadata.NodeInfo `json:"slaves"`
}

type ShardSlotsParam struct {
	Slots []string `json:"slots" validate:"required"`
}

type MigrateSlotsDataParam struct {
	Tasks []*etcd.MigrateTask `json:"tasks" validate:"required"`
}

type MigrateSlotsParam struct {
	SourceShardIdx int                  `json:"source" validate:"required"`
	TargetShardIdx int                  `json:"target" validate:"required"`
	SlotRanges     []metadata.SlotRange `json:"slots" validate:"required"`
}

func GetAPIV1PrefixURL(addr string) string {
	return "http://" + addr + "/api/v1"
}

func GetControllerLeaderURL(addr string) string {
	return GetAPIV1PrefixURL(addr) + "/controller/leader"
}

func GetNamespaceRootURL(addr string) string {
	return GetAPIV1PrefixURL(addr) + "/namespaces"
}

func GetNamespaceURL(addr, ns string) string {
	return GetNamespaceRootURL(addr) + "/" + ns
}

func GetClusterRootURL(addr, ns string) string {
	return GetNamespaceURL(addr, ns) + "/clusters"
}

func GetClusterURL(addr, ns, cluster string) string {
	return GetClusterRootURL(addr, ns) + "/" + cluster
}

func GetClusterFailoverURL(addr, ns, cluster, queryType string) string {
	return GetClusterRootURL(addr, ns) + "/" + cluster + "/failover/" + queryType
}

func GetClusterMigrateURL(addr, ns, cluster, queryType string) string {
	return GetClusterRootURL(addr, ns) + "/" + cluster + "/migration/" + queryType
}

func GetShardRootURL(addr, ns, cluster string) string {
	return GetClusterURL(addr, ns, cluster) + "/shards"
}

func GetMigrateURL(addr, ns, cluster string) string {
	return GetShardRootURL(addr, ns, cluster) + "/migration/slot_and_data"
}

func GetMigrateSlotsURL(addr, ns, cluster string) string {
	return GetShardRootURL(addr, ns, cluster) + "/migration/slot_only"
}

func GetShardURL(addr, ns, cluster string, shardIdx int) string {
	return GetShardRootURL(addr, ns, cluster) + "/" + strconv.Itoa(shardIdx)
}

func GetSlotURL(addr, ns, cluster string, shardIdx int) string {
	return GetShardURL(addr, ns, cluster, shardIdx) + "/slots"
}

func GetNodeRootURL(addr, ns, cluster string, shardIdx int) string {
	return GetShardURL(addr, ns, cluster, shardIdx) + "/nodes"
}

func GetNodeURL(addr, ns, cluster string, shardIdx int, nodeID string) string {
	return GetNodeRootURL(addr, ns, cluster, shardIdx) + "/" + nodeID
}

func GetFailoverNodeURL(addr, ns, cluster string, shardIdx int, nodeID string) string {
	return GetNodeRootURL(addr, ns, cluster, shardIdx) + "/" + nodeID + "/failover"
}
