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

var SendCommandToClusterCommand = cli.Command{
	Name:      "send_command_to_cluster",
	Usage:     "Send command to cluster nodes",
	ArgsUsage: "${redis_command} ${args}...",
	Action:    pdoAction,
	Description: `
    Send redis command to cluster nodes
    `,
}

func pdoAction(c *cli.Context) {
	if len(c.Args()) < 1 {
		fmt.Println("Required at least 1 param")
		return
	}
	ctx := clictx.GetContext()
	if ctx.Location != clictx.LocationCluster {
		fmt.Println("Command send_command_to_cluster should be under culster dir")
		return
	}

	var redisArgs []interface{}
	for _, arg := range c.Args() {
		redisArgs = append(redisArgs, arg)
	}

	// access and parser cluster info
	resp, err := util.HttpGet(handlers.GetClusterURL(ctx.Leader, ctx.Namespace, ctx.Cluster), nil, 0)
	if responseError("Get cluster", resp, err) {
		return
	}
	var cluster metadata.Cluster
	err = util.InterfaceToStruct(resp.Body, &cluster)
	if err != nil {
		fmt.Println("Internal error: ", err)
		return
	}

	for _, shard := range cluster.Shards {
		for _, node := range shard.Nodes {
			client, err := util.NewRedisClient(node.Address)
			if err != nil {
				fmt.Printf("addr: %s, dail error : %s\n", node.Address, err.Error())
				continue
			}
			if res, err := client.Do(context.Background(), redisArgs...).Result(); err != nil {
				// FIXME: log error here
				fmt.Println("do error: ", err)
			} else {
				fmt.Printf(node.Address + ": ")
				fmt.Println(res)
			}
		}
	}
	return
}
