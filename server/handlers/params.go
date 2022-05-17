package handlers

import(
	"strconv"
	"github.com/KvrocksLabs/kvrocks-controller/metadata"
)

type Response struct {
	Errno  int         `json:"errno"`
	Errmsg string      `json:"errmsg"`
	Body   interface{} `json:"body"`
}

const (
	Success   = 0
	Unsuccess = 777
)
func MakeResponse(errno int, msg string, body interface{}) Response {
	return Response{errno, msg, body}
}

func MakeSuccessResponse(body interface{}) Response {
	return MakeResponse(Success, "OK", body)
}

func MakeFailureResponse(msg string) Response {
	return MakeResponse(Unsuccess, msg, nil)
}

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