package command

import (
	"fmt"
	"time"

	"github.com/KvrocksLabs/kvrocks_controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks_controller/server/handlers"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"gopkg.in/urfave/cli.v1"
)

var DelShardCommand = cli.Command{
	Name:      "del_shard",
	Usage:     "Delete the shard",
	ArgsUsage: "-s ${shard}",
	Action:    delShardAction,
	Flags: []cli.Flag{
		cli.IntFlag{
			Name:  "s,shard",
			Value: -1,
			Usage: "Shard index"},
	},
	Description: `
    Delete shard should be under the cluster dir
    `,
}

func delShardAction(c *cli.Context) {
	ctx := context.GetContext()
	if ctx.Location != context.LocationCluster {
		fmt.Println("Command del_command should be under cluster dir")
		return
	}
	shardIdx := c.Int("s")
	if shardIdx < 0 {
		fmt.Println("Invalid shard index")
		return
	}
	resp, err := util.HttpDelete(handlers.GetShardURL(ctx.Leader, ctx.Namespace, ctx.Cluster, shardIdx), nil, 5*time.Second)
	HttpResponeException("Delete", resp, err)
}
