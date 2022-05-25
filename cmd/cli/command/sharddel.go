package command

import (
	"fmt"
	"time"

	"gopkg.in/urfave/cli.v1"
	"github.com/KvrocksLabs/kvrocks-controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks-controller/util"
	"github.com/KvrocksLabs/kvrocks-controller/server/handlers"
)

var DelShardCommand = cli.Command{
	Name:      "delshard",
	Usage:     "del shard",
	ArgsUsage: "-si ${shard_idx}",
	Action:    delShardAction,
	Flags: []cli.Flag{
		cli.IntFlag{
			Name:  "si,shardidx", 
			Value: -1, 
			Usage: "shard number"},
	},
	Description: `
    del shard under the special cluster
    `,
}

func delShardAction(c *cli.Context) {
	ctx := context.GetContext()
	if ctx.Location != context.LocationCluster {
		fmt.Println("delshard command should under clsuter dir")
		return 
	}
	shardIdx := c.Int("si")
	if shardIdx < 0 {
		fmt.Println("shard_idx(-i) error")
		return
	}
	resp, err := util.HttpDelete(handlers.GetShardURL(ctx.Leader, ctx.Namespace, ctx.Cluster, shardIdx), nil, 5 * time.Second)
	HttpResponeException("dellete shard", resp, err)
}