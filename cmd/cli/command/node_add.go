package command

import (
	"context"
	"fmt"
	"time"

	clictx "github.com/KvrocksLabs/kvrocks_controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/server/handlers"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"gopkg.in/urfave/cli.v1"
)

var AddNodeCommand = cli.Command{
	Name:      "add_node",
	Usage:     "Add node",
	ArgsUsage: "-si ${shard} -n ${node_addr}",
	Action:    addNode,
	Flags: []cli.Flag{
		cli.IntFlag{
			Name:  "s,shard",
			Value: -1,
			Usage: "Shard index"},
		cli.StringFlag{
			Name:  "n,node",
			Value: "",
			Usage: "Kvrocks node address"},
	},
	Description: `
    add node to the special shard
    `,
}

func addNode(c *cli.Context) {
	ctx := clictx.GetContext()
	if ctx.Location != clictx.LocationCluster {
		fmt.Println("Command `add_node` should under cluster dir")
		return
	}

	// check parameter
	shardIdx := c.Int("s")
	nodeAddr := c.String("n")
	if shardIdx < 0 {
		fmt.Println("Shard index error")
		return
	}

	// ping node
	client, err := util.RedisPool(nodeAddr)
	if err != nil {
		fmt.Printf("addr: %s, dail error : %s\n", nodeAddr, err.Error())
		return
	}
	_, err = client.Do(context.Background(), "ping").Result()
	if err != nil {
		fmt.Println("Node: ", nodeAddr, " ping err: ", err)
		return
	}

	// only add slave node
	node := &metadata.NodeInfo{
		ID:        accessNodeID(nodeAddr),
		CreatedAt: time.Now().Unix(),
		Address:   nodeAddr,
		Role:      metadata.RoleSlave,
	}
	resp, err := util.HttpPost(handlers.GetNodeRootURL(ctx.Leader, ctx.Namespace, ctx.Cluster, shardIdx), node, 5*time.Second)
	HttpResponeException("Add node", resp, err)
}
