package command

import (
	"context"

	clictx "github.com/KvrocksLabs/kvrocks_controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/server/handlers"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"gopkg.in/urfave/cli.v1"
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
		return
	}

	node := c.String("n")
	if len(node) == 0 {
		return
	}

	resp, err := util.HttpGet(handlers.GetClusterURL(ctx.Leader, ctx.Namespace, ctx.Cluster), nil, 0)
	if HttpResponeException("get cluster", resp, err) {
		return
	}

	// parser cluster info from interface
	var cluster metadata.Cluster
	err = util.InterfaceToStruct(resp.Body, &cluster)
	if err != nil {
		return
	}

	clusterStr, err := cluster.ToSlotString()
	if err != nil {
		return
	}

	client, err := util.RedisPool(node)
	if err != nil {
		return
	}
	if err := client.Do(context.Background(), "CLUSTERX", "setnodeid", accessNodeID(node)).Err(); err != nil {
		return
	}
	if err = client.Do(context.Background(), "CLUSTERX", "setnodes", clusterStr, cluster.Version).Err(); err != nil {
		return
	}
	return
}
