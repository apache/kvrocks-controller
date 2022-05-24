package command

import (
	"fmt"
	"context"

	"gopkg.in/urfave/cli.v1"
	"github.com/KvrocksLabs/kvrocks-controller/server/handlers"
	clictx "github.com/KvrocksLabs/kvrocks-controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks-controller/util"
	"github.com/KvrocksLabs/kvrocks-controller/metadata"
)

var RedisPdoCommand = cli.Command{
	Name:      "pdo",
	Usage:     "do redis command to cluster nodes",
	ArgsUsage: "${redis_command} ${args}...",
	Action:    pdoAction,
	Description: `
    send redis command to cluster nodes
    `,
}

func pdoAction(c *cli.Context) {
	if len(c.Args()) < 1 {
    	fmt.Println("do command at least 1 params")
    	return 
    }
	ctx := clictx.GetContext()
	if ctx.Location != clictx.LocationCluster {
		fmt.Println("pdo command should under clsuter dir")
		return 
	}
    
    var redisArgs []interface{}
    for _, arg := range c.Args() {
    	redisArgs = append(redisArgs, arg)
    }

    // access and parser cluster info
    resp, err := util.HttpGet(handlers.GetClusterURL(ctx.Leader, ctx.Namespace,ctx.Cluster), nil, 0)
	if HttpResponeException("get cluster", resp, err) {
		return
	}
	var cluster metadata.Cluster
	err = util.InterfaceToStruct(resp.Body, &cluster)
	if err != nil {
		fmt.Println("response transfer struct error: ", err)
		return
	}

	for _, shard :=range cluster.Shards {
		for _, node :=range shard.Nodes {
		    client, err := util.RedisPool(node.Address)
			if err != nil {
				fmt.Println("addr: %s, dail error : %w", node.Address, err)
				continue
			}
		    res, err := client.Do(context.Background(), redisArgs...).Result()
		    if err != nil {
				fmt.Println("do error: ", err)
			} else {
				fmt.Println(res)
			}
		}
	}
	return 
}