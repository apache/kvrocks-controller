package command

import (
	"fmt"
	"time"

	"gopkg.in/urfave/cli.v1"
	"github.com/KvrocksLabs/kvrocks-controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks-controller/util"
	"github.com/KvrocksLabs/kvrocks-controller/server/handlers"
)

var FailoverCommand = cli.Command{
	Name:      "failover",
	Usage:     "failover node",
	ArgsUsage: "-si ${shard_idx} -ni ${nodeid}",
	Action:    failoverAction,
	Flags: []cli.Flag{
		cli.IntFlag{
			Name:  "si,shardidx", 
			Value: -1, 
			Usage: "shard number"},
		cli.StringFlag{
			Name:  "ni,nodeid",
			Value: "",
			Usage: "kvrocks node id"},
	},
	Description: `
    failover node
    `,
}

func failoverAction(c *cli.Context) {
	ctx := context.GetContext()
	if ctx.Location != context.LocationCluster {
		fmt.Println("failover command should under clsuter dir")
		return 
	}

	// check parameter
	shardIdx := c.Int("si")
	nodeID := c.String("ni")
	if shardIdx < 0 {
		fmt.Println("shard_idx(-i) error")
		return
	}

	resp, err := util.HttpPost(handlers.GetFailoverNodeURL(ctx.Leader, ctx.Namespace, ctx.Cluster, shardIdx, nodeID), nil, 5 * time.Second)
	HttpResponeException("failover node", resp, err)
}