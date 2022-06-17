package command

import (
	"fmt"
	"time"

	"github.com/KvrocksLabs/kvrocks_controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks_controller/server/handlers"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"gopkg.in/urfave/cli.v1"
)

var DelNodeCommand = cli.Command{
	Name:      "del_node",
	Usage:     "Delete node",
	ArgsUsage: "-s ${shard} -n ${node}",
	Action:    deleteNode,
	Flags: []cli.Flag{
		cli.IntFlag{
			Name:  "s,shard",
			Value: -1,
			Usage: "shard index"},
		cli.StringFlag{
			Name:  "n,node",
			Value: "",
			Usage: "Kvrocks node id"},
	},
	Description: `Del node should be under the special shard`,
}

func deleteNode(c *cli.Context) {
	ctx := context.GetContext()
	if ctx.Location != context.LocationCluster {
		fmt.Println("Delete node should be under the special shard")
		return
	}

	shardIdx := c.Int("si")
	nodeID := c.String("ni")
	if shardIdx < 0 {
		fmt.Println("shard_idx(-i) error")
		return
	}

	resp, err := util.HttpDelete(handlers.GetNodeURL(ctx.Leader, ctx.Namespace, ctx.Cluster, shardIdx, nodeID), nil, 5*time.Second)
	if HttpResponeException("delete node", resp, err) {
		return
	}
}
