package command

import (
	"fmt"
	"context"

	"gopkg.in/urfave/cli.v1"
	"github.com/KvrocksLabs/kvrocks-controller/metadata"
	"github.com/KvrocksLabs/kvrocks-controller/util"
	clictx "github.com/KvrocksLabs/kvrocks-controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks-controller/server/handlers"
)

var SyncCommand = cli.Command{
	Name:      "sync",
	Usage:     "sync node topo",
	ArgsUsage: "-n ${node_addr}",
	Action:    syncAction,
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "n,node",
			Value: "",
			Usage: "kvrocks node addr"},
	},
	Description: `
    sync cluster topo metadata to node
    `,
}

func syncAction(c *cli.Context) {
	ctx := clictx.GetContext()
	if ctx.Location != clictx.LocationCluster {
		fmt.Println("sync command should under clsuter dir")
		return 
	}

	node := c.String("n")
	if len(node) == 0 {
		fmt.Println("sync command node(-n) should be set")
		return
	}

	resp, err := util.HttpGet(handlers.GetClusterURL(ctx.Leader, ctx.Namespace,ctx.Cluster), nil, 0)
	if HttpResponeException("get cluster", resp, err) {
		return
	}

	// parser cluster info from interface
	var cluster metadata.Cluster
	err = util.InterfaceToStruct(resp.Body, &cluster)
	if err != nil {
		fmt.Println("response transfer struct error: ", err)
		return
	}

	clusterStr, err := cluster.ToSlotString()
	if err != nil {
		fmt.Println("cluster to string error: ", err)
		return 
	}

    client, err := util.RedisPool(node)
	if err != nil {
		fmt.Println("addr: %s, dail error : %w", node, err)
		return
	}
	if err := client.Do(context.Background(), "CLUSTERX", "setnodeid", accessNodeID(node)).Err(); err != nil {
		fmt.Println("clusterx setnodeid error: ", err)
		return 
	}
	if err = client.Do(context.Background(), "CLUSTERX", "setnodes", clusterStr, cluster.Version).Err() ; err != nil {
		fmt.Println("clusterx setnodes error: ", err)
		return 
	}
	fmt.Println("OK")
	return
}