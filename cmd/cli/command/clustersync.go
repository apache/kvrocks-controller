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

var PsyncCommand = cli.Command{
	Name:      "psync",
	Usage:     "sync topo to cluster nodes",
	Action:    psyncAction,
	Description: `
    sync cluster topo metadata to cluster nodes
    `,
}

func psyncAction(c *cli.Context) {
	ctx := clictx.GetContext()
	if ctx.Location != clictx.LocationCluster {
		fmt.Println("psync command should under clsuter dir")
		return 
	}

	// access cluster info
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

	for _, shard :=range cluster.Shards {
		for _, node :=range shard.Nodes {
		    client, err := util.RedisPool(node.Address)
			if err != nil {
				fmt.Println("addr: %s, dail error : %w", node.Address, err)
				continue
			}
		    if err := client.Do(context.Background(), "CLUSTERX", "setnodeid", node.ID).Err(); err != nil {
				fmt.Println(node.Address + " clusterx setnodeid error: ", err)
				continue
			}
			if err = client.Do(context.Background(), "CLUSTERX", "setnodes", clusterStr, cluster.Version).Err() ; err != nil {
				fmt.Println(node.Address + " clusterx setnodes error: ", err)
				continue 
			}
			fmt.Println(node.Address + " OK")
		}
	}
	return
}