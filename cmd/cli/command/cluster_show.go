package command

import (
	"fmt"

	"github.com/KvrocksLabs/kvrocks_controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/server/handlers"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"gopkg.in/urfave/cli.v1"
)

const (
	statusOK     = "OK"
	statusFailed = "Failed"
)

var ShowClusterCommand = cli.Command{
	Name:        "show_cluster",
	Usage:       "Show the cluster topo info",
	Action:      showClusterAction,
	Description: `Show cluster topo info under special cluster`,
}

var (
	showFields = []string{"ID", "Status", "Role", "NodeId", "GitSha1", "Addr", "Slots", "Epoch",
		"Connected", "Repl", "Clients", "OPS", "Mem", "Disk", "NetIn", "NetOut"}
)

type ShowNode struct {
	ID        int
	Status    string
	Role      string
	NodeId    string
	GitSha1   string
	Addr      string
	Slots     string
	Epoch     int64
	Connected string
	Repl      string
	Clients   string
	Mem       string
	Ops       string
	NetIn     string
	NetOut    string
	Disk      string
}

func showClusterAction(c *cli.Context) {
	ctx := context.GetContext()
	if ctx.Location != context.LocationCluster {
		fmt.Println("Command show_cluster should be run under the cluster dir")
		return
	}

	resp, err := util.HttpGet(handlers.GetClusterURL(ctx.Leader, ctx.Namespace, ctx.Cluster), nil, 0)
	if responseError("Show cluster", resp, err) {
		return
	}

	// parser cluster info from interface
	var cluster metadata.Cluster
	err = util.InterfaceToStruct(resp.Body, &cluster)
	if err != nil {
		fmt.Println("Internal error: ", err)
		return
	}

	var allNodes []*ShowNode
	for i, shard := range cluster.Shards {
		slotStr := ""
		for _, slot := range shard.SlotRanges {
			if len(slotStr) != 0 {
				slotStr = slotStr + ","
			}
			slotStr += slot.String()
		}
		for _, n := range shard.Nodes {
			node := &ShowNode{
				ID:     i,
				Role:   n.Role,
				NodeId: n.ID[0:8],
				Addr:   n.Address,
				Slots:  slotStr,
				Status: statusOK,
			}
			info, err := util.NodeInfoCmd(n.Address)
			if err != nil {
				node.Status = statusFailed
			} else {
				node.GitSha1 = info.Server.GitSha1
				node.Clients = info.Client.ConnectedClients + "/" + info.Client.MaxClients
				node.Mem = info.Mem.UsedMemoryHuman
				node.Ops = info.States.InstantaneousOps
				node.NetIn = info.States.InstantaneousInputKbps
				node.NetOut = info.States.InstantaneousOutputKbps
				node.Disk = info.KeySpace.UsedDiskPercent
				if n.Role == metadata.RoleMaster {
					node.Connected = info.MasterReplication.ConnectedSlaves
					node.Repl = info.MasterReplication.MasterReplOffset
				} else {
					node.Connected = info.SlaveReplication.MasterLinkStatus
					node.Repl = info.SlaveReplication.SlaveReplOffset
				}
			}
			clusterInfo, err := util.ClusterInfoCmd(n.Address)
			if err != nil {
				node.Status = statusFailed
			} else {
				node.Epoch = clusterInfo.ClusterMyEpoch
			}
			allNodes = append(allNodes, node)
		}
		allNodes = append(allNodes, nil)
	}
	allNodes = allNodes[0 : len(allNodes)-1]
	util.PrintTable(showFields, nodesToInterfaceSlice(allNodes))
	return
}
