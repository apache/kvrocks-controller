package handlers

import(
	"strconv"
	"github.com/KvrocksLabs/kvrocks-controller/metadata"
	"github.com/KvrocksLabs/kvrocks-controller/storage/base/etcd"
)

type CreateNamespaceParam struct {
	Namespace string `json:namespace`
}

type CreateClusterParam struct {
	Cluster string               `json:"cluster"`
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
	SourceShardIdx int `json:"source" validate:"required"`
	TargetShardIdx int `json:"target" validate:"required"`
	SlotRanges     []metadata.SlotRange `json:"slots" validate:"required"`
}

func GetApiv1PrefixURL(addr string) string {
	return "http://" + addr + "/api/v1"
}

func GetControllerLeaderURL(addr string) string {
	return GetApiv1PrefixURL(addr) + "/controller/leader"
}

func GetNamespaceRootURL(addr string) string {
	return GetApiv1PrefixURL(addr) + "/namespaces"
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

func GetShardRootURL(addr, ns, cluster string) string {
	return GetClusterURL(addr, ns, cluster) + "/shards"
}

func GetMigrateURL(addr, ns, cluster string) string {
	return GetShardRootURL(addr, ns, cluster) + "/migrate"
}

func GetMigrateSlotsURL(addr, ns, cluster string) string {
	return GetShardRootURL(addr, ns, cluster) + "/migrateslots"
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

func GetNodeURL(addr, ns, cluster string, shardIdx int, nodeid string) string {
	return GetNodeRootURL(addr, ns, cluster, shardIdx) + "/" + nodeid
}