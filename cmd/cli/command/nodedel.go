package command

import (
	"fmt"
	"time"

	"gopkg.in/urfave/cli.v1"
	"github.com/KvrocksLabs/kvrocks-controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks-controller/util"
	"github.com/KvrocksLabs/kvrocks-controller/server/handlers"
)

var DelNodeCommand = cli.Command{
	Name:      "delnode",
	Usage:     "del node",
	ArgsUsage: "-si ${shard_idx} -ni ${nodeid}",
	Action:    delNodeAction,
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
    del node under special shard
    `,
}

func delNodeAction(c *cli.Context) {
	ctx := context.GetContext()
	if ctx.Location != context.LocationCluster {
		fmt.Println("mkcl command should under clsuter dir")
		return 
	}

	shardIdx := c.Int("si")
	nodeID := c.String("ni")
	if shardIdx < 0 {
		fmt.Println("shard_idx(-i) error")
		return
	}

	resp, err := util.HttpDelete(handlers.GetNodeURL(ctx.Leader, ctx.Namespace, ctx.Cluster, shardIdx, nodeID), nil, 5 * time.Second)
	if HttpResponeException("delete node", resp, err) {
		return
	}
}