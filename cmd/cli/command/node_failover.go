package command

import (
	"fmt"
	"time"

	"github.com/KvrocksLabs/kvrocks_controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks_controller/server/handlers"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"gopkg.in/urfave/cli.v1"
)

var FailoverCommand = cli.Command{
	Name:      "failover",
	Usage:     "Failover node",
	ArgsUsage: "-s ${shard} -n ${node}",
	Action:    failover,
	Flags: []cli.Flag{
		cli.IntFlag{
			Name:  "s,shard",
			Value: -1,
			Usage: "Shard index"},
		cli.StringFlag{
			Name:  "n,node",
			Value: "",
			Usage: "Kvrocks node id"},
	},
	Description: "Failover the cluster node",
}

func failover(c *cli.Context) {
	ctx := context.GetContext()
	if ctx.Location != context.LocationCluster {
		fmt.Println("Command failover should be under the cluster node")
		return
	}

	shardIdx := c.Int("s")
	nodeID := c.String("n")
	if shardIdx < 0 {
		fmt.Println("Shard index error")
		return
	}

	resp, err := util.HttpPost(handlers.GetFailoverNodeURL(ctx.Leader, ctx.Namespace, ctx.Cluster, shardIdx, nodeID), nil, 5*time.Second)
	responseError("Failover node", resp, err)
}
