package command

import (
	"fmt"
	"time"
	"context"
	"crypto/sha1"
    "encoding/hex"

	"github.com/go-redis/redis/v8"
	"gopkg.in/urfave/cli.v1"
	cliCtx "github.com/KvrocksLabs/kvrocks-controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks-controller/util"
	"github.com/KvrocksLabs/kvrocks-controller/server/handlers"
	"github.com/KvrocksLabs/kvrocks-controller/metadata"
)

var AddNodeCommand = cli.Command{
	Name:      "addnode",
	Usage:     "add node",
	ArgsUsage: "-i ${shard_idx} -n ${nodeaddr}",
	Action:    addNodeAction,
	Flags: []cli.Flag{
		cli.IntFlag{
			Name:  "i,shardidx", 
			Value: -1, 
			Usage: "shard number"},
		cli.StringFlag{
			Name:  "n,node",
			Value: "",
			Usage: "kvrocks node address"},
	},
	Description: `
    add node under special shard
    `,
}

func addNodeAction(c *cli.Context) {
	ctx := cliCtx.GetContext()
	if ctx.Location != cliCtx.LocationCluster {
		fmt.Println("mkcl command should under clsuter dir")
		return 
	}
	shardIdx := c.Int("i")
	nodeAddr := c.String("n")
	if shardIdx < 0 {
		fmt.Println("shard_idx(-i) error")
		return
	}
	client := redis.NewClient(&redis.Options{
        Addr: nodeAddr,
    })
    _, err := client.Do(context.Background(), "ping").Result()
    if err != nil {
    	fmt.Println("node: ", nodeAddr, " ping err: ", err)
    	return 
    }
    sha := sha1.New()
	sha.Write([]byte(nodeAddr))
	bytes := sha.Sum(nil)
	nodeID := hex.EncodeToString(bytes)
    node := &metadata.NodeInfo{
    	ID:        nodeID,
		CreatedAt: time.Now().Unix(),
		Address:   nodeAddr,
		Role:      metadata.RoleSlave,
    }
    resp, err := util.HttpPost(handlers.GetNodeRootURL(ctx.Leader, ctx.Namespace, ctx.Cluster, shardIdx), node, 5 * time.Second)
	if err != nil {
		fmt.Println("add node error: " + err.Error())
		return 
	}
	if resp.Errno != handlers.Success {
		fmt.Println("add node error: " + resp.Errmsg)	
		return
	}
	if resp.Body == nil {
		fmt.Println("add node error")
		return
	}
	fmt.Println(resp.Body.(string))
}

var DelNodeCommand = cli.Command{
	Name:      "delnode",
	Usage:     "del node",
	ArgsUsage: "-i ${shard_idx} -n ${nodeid}",
	Action:    delNodeAction,
	Flags: []cli.Flag{
		cli.IntFlag{
			Name:  "i,shardidx", 
			Value: -1, 
			Usage: "shard number"},
		cli.StringFlag{
			Name:  "n,node",
			Value: "",
			Usage: "kvrocks node address"},
	},
	Description: `
    del node under special shard
    `,
}

func delNodeAction(c *cli.Context) {
	ctx := cliCtx.GetContext()
	if ctx.Location != cliCtx.LocationCluster {
		fmt.Println("mkcl command should under clsuter dir")
		return 
	}
	shardIdx := c.Int("i")
	nodeID := c.String("n")
	if shardIdx < 0 {
		fmt.Println("shard_idx(-i) error")
		return
	}
	resp, err := util.HttpDelete(handlers.GetNodeURL(ctx.Leader, ctx.Namespace, ctx.Cluster, shardIdx, nodeID), nil, 5 * time.Second)
	if err != nil {
		fmt.Println("delete node error: " + err.Error())
		return 
	}
	if resp.Errno != handlers.Success {
		fmt.Println("delete node error: " + resp.Errmsg)	
		return
	}
	if resp.Body == nil {
		fmt.Println("delete node error")
		return
	}
	fmt.Println(resp.Body.(string))
}