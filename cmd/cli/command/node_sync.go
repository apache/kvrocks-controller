package command

import (
	"context"
	"fmt"

	clictx "github.com/KvrocksLabs/kvrocks_controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/server/handlers"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"gopkg.in/urfave/cli.v1"
)

var SyncTopoToNodeCommand = cli.Command{
	Name:      "sync_topo_to_node",
	Usage:     "sync the cluster topo to node",
	ArgsUsage: "-n ${node_addr}",
	Action:    syncTopoToNode,
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "n,node",
			Value: "",
			Usage: "Kvrocks node addr"},
	},
	Description: `
    sync the cluster topo to node
    `,
}

func syncTopoToNode(c *cli.Context) {
	ctx := clictx.GetContext()
	if ctx.Location != clictx.LocationCluster {
		fmt.Println("Command sync_topo_to_node should under the cluster dir")
		return
	}

	node := c.String("n")
	if len(node) == 0 {
		fmt.Println("Missing node address")
		return
	}

	resp, err := util.HttpGet(handlers.GetClusterURL(ctx.Leader, ctx.Namespace, ctx.Cluster), nil, 0)
	if responseError("Get cluster", resp, err) {
		return
	}

	// parser cluster info from interface
	var cluster metadata.Cluster
	err = util.InterfaceToStruct(resp.Body, &cluster)
	if err != nil {
		fmt.Println("Internal error: ", err)
		return
	}

	clusterStr, err := cluster.ToSlotString()
	if err != nil {
		fmt.Println("Cluster to string error: ", err)
		return
	}

	client, err := util.RedisPool(node)
	if err != nil {
		fmt.Printf("addr: %s, dail error : %s\n", node, err.Error())
		return
	}
	if err := client.Do(context.Background(), "CLUSTERX", "setnodeid", accessNodeID(node)).Err(); err != nil {
		fmt.Println("Command clusterx setnodeid error: ", err)
		return
	}
	if err = client.Do(context.Background(), "CLUSTERX", "setnodes", clusterStr, cluster.Version).Err(); err != nil {
		fmt.Println("Command clusterx setnodes error: ", err)
		return
	}
	fmt.Println("OK")
	return
}
