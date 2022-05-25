package command

import (
	"fmt"

	"gopkg.in/urfave/cli.v1"
	"github.com/KvrocksLabs/kvrocks-controller/server/handlers"
	"github.com/KvrocksLabs/kvrocks-controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks-controller/metadata"
	"github.com/KvrocksLabs/kvrocks-controller/util"
)

var ShowClusterCommand = cli.Command{
	Name:      "showcluster",
	ShortName: "showc",
	Usage:     "show cluster topo info",
	Action:    showClusterAction,
	Description: `
    show cluster topo info under special cluster
    `,
}

var (
	showItems = []string{"ID", "Status", "Role", "NodeId", "GitSha1", "Addr", "Slots", "Epoch", 
						"Connectd", "Repl", "Clients", "Ops", "Mem", "Disk", "NetIn", "Netout"}
)

type ShowNode struct {
	ID       int
	Status   string
	Role     string
	NodeId   string
	GitSha1  string
	Addr     string
	Slots    string
	Epoch    int64
	Connectd string
	Repl     string
	Clients  string
	Mem      string
	Ops      string
	NetIn    string
	Netout   string
	Disk     string
}

func showClusterAction(c *cli.Context) {
	ctx := context.GetContext()
	if ctx.Location != context.LocationCluster {
		fmt.Println("showcluster command should under special cluster dir")
		return 
	}

	resp, err := util.HttpGet(handlers.GetClusterURL(ctx.Leader, ctx.Namespace,ctx.Cluster), nil, 0)
	if HttpResponeException("show cluster", resp, err) {
		return
	}

	// parser cluster info from interface
	var cluster metadata.Cluster
	err = util.InterfaceToStruct(resp.Body, &cluster)
	if err != nil {
		fmt.Println("response transfer struct error: ", err)
		return
	}

	var allNodes []*ShowNode
	for i, shard :=range cluster.Shards {
		slotStr := ""
		for _, slot :=range shard.SlotRanges {
			if len(slotStr) != 0 {
				slotStr = slotStr + ","
			}
			slotStr += slot.String()
		}
		for _, n :=range shard.Nodes {
			node := &ShowNode{
				ID:     i,
				Role:   n.Role,
				NodeId: n.ID[0:8],
				Addr:   n.Address,
				Slots:  slotStr,
				Status: "OK",
			}
			info, err := util.NodeInfoCmd(n.Address)
			if err != nil {
				node.Status = "fail"
			} else {
				node.GitSha1 = info.Server.GitSha1
				node.Clients = info.Client.ConnectedClients + "/" + info.Client.MaxClients
				node.Mem = info.Mem.UsedMemoryHuman
				node.Ops = info.States.InstantaneousOps
				node.NetIn = info.States.InstantaneousInputKbps
				node.Netout = info.States.InstantaneousOutputKbps
				node.Disk = info.KeySpace.UsedDiskPercent
				if n.Role == metadata.RoleMaster {
					node.Connectd = info.MasterReplication.ConnectedSlaves
					node.Repl = info.MasterReplication.MasterReplOffset
				} else {
					node.Connectd = info.SlaveReplication.MasterLinkStatus
					node.Repl = info.SlaveReplication.SlaveReplOffset
				}
			}
			clusterInfo, err := util.ClusterInfoCmd(n.Address)
			if err != nil {
				node.Status = "fail"
			} else {
				node.Epoch = clusterInfo.ClusterMyEpoch
			}
			allNodes = append(allNodes, node)
		}
		allNodes = append(allNodes, nil)
	}
	allNodes = allNodes[0: len(allNodes)-1]

	util.PrintTable(showItems, nodesToInterfaceSlice(allNodes))
	return 
}