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

var SyncClusterTopoCommand = cli.Command{
	Name:   "sync_topo_to_cluster",
	Usage:  "Sync topo to cluster nodes",
	Action: syncClusterTopo,
	Description: `
    sync cluster topo metadata to cluster nodes
    `,
}

func syncClusterTopo(c *cli.Context) {
	ctx := clictx.GetContext()
	if ctx.Location != clictx.LocationCluster {
		fmt.Println("Command sync_topo_to_cluster should under the cluster dir")
		return
	}

	resp, err := util.HttpGet(handlers.GetClusterURL(ctx.Leader, ctx.Namespace, ctx.Cluster), nil, 0)
	if HttpResponeException("Get cluster info", resp, err) {
		return
	}

	// parser cluster info from interface
	var cluster metadata.Cluster
	err = util.InterfaceToStruct(resp.Body, &cluster)
	if err != nil {
		fmt.Println("Internal error: ", err)
		return
	}
	clusterStr, err := cluster.ToSlotString()
	if err != nil {
		fmt.Println("Cluster tostring error: ", err)
		return
	}

	for _, shard := range cluster.Shards {
		for _, node := range shard.Nodes {
			client, err := util.RedisPool(node.Address)
			if err != nil {
				fmt.Printf("addr: %s, dail error : %s\n", node.Address, err.Error())
				continue
			}
			if err := client.Do(context.Background(), "CLUSTERX", "setnodeid", node.ID).Err(); err != nil {
				fmt.Println(node.Address+" clusterx setnodeid error: ", err)
				continue
			}
			if err = client.Do(context.Background(), "CLUSTERX", "setnodes", clusterStr, cluster.Version).Err(); err != nil {
				fmt.Println(node.Address+" clusterx setnodes error: ", err)
				continue
			}
			fmt.Println(node.Address + ": OK")
		}
	}
	return
}
