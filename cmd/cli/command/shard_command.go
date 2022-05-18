package command

import (
	"fmt"
	"os"
	"bufio"
	"sort"
	"time"
	"strings"

	"gopkg.in/urfave/cli.v1"
	cliCtx "github.com/KvrocksLabs/kvrocks-controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks-controller/util"
	"github.com/KvrocksLabs/kvrocks-controller/server/handlers"
)

var AddShardCommand = cli.Command{
	Name:      "addshard",
	Usage:     "add shards",
	ArgsUsage: "-s ${shard_number} -n ${nodeaddr1,nodeaddr2...}/-c ${configpath} -d ${do}",
	Action:    addShardAction,
	Flags: []cli.Flag{
		cli.IntFlag{
			Name:  "s,shard", 
			Value: 1, 
			Usage: "shard number"},
		cli.StringFlag{
			Name:  "n,nodes",
			Value: "",
			Usage: "kvrocks nodes address"},
		cli.StringFlag{
			Name:  "c,config",
			Value: "",
			Usage: "config path, kvrocks nodes address"},
		cli.BoolFlag{
			Name:  "d,do",
			Usage: "flag do init cluster"},
	},
	Description: `
    add shards under special cluster
    `,
}

func addShardAction(c *cli.Context) {
	ctx := cliCtx.GetContext()
	if ctx.Location != cliCtx.LocationCluster {
		fmt.Println("mkcl command should under clsuter dir")
		return 
	}
	shard       := c.Int("s")
	conf        := c.String("c")
	addrs       := c.String("n")
	do 	        := c.Bool("d")
	if len(conf) != 0 && len(addrs) != 0 {
		fmt.Println("config path(-c) or nodes address(-n), cannot be set at the same time")
		return 
	}
	if conf == "" && addrs == "" { 
		fmt.Println("config path(-c) or nodes address(-n), at least one be set")
		return 
	}
	var nodes []string
	if conf != "" {
		// accquire nodes address
		file, err := os.Open(conf)
		if err != nil {
			fmt.Println("open config file err: ", err)
			return
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
	        line := scanner.Text()
	   		nodes = append(nodes, line)
	    }
	    if err := scanner.Err(); err != nil {
	    	fmt.Println("scan config file err: ", err)
			return
	    }
	} else {
		nodes = strings.Split(addrs, ",")
	}

	nodeSize := len(nodes)
	if nodeSize == 0 {
    	fmt.Println("nodes is empty")
		return
    }
	if nodeSize < shard {
    	fmt.Println("nodes less shard number")
		return
    }
    if nodeSize % shard != 0 {
    	fmt.Println("nodes can't divide shard number")
		return
    }
    sort.Strings(nodes)
    cluster := GenerateCluster(nodes, shard, false)
	if cluster == nil {
		return 
	}
	clusterStr, err := cluster.ToSlotString()
	if err != nil {
		fmt.Println("cluster to string error: ", err)
		return
	}
	fmt.Println("add shards plan:")
	fmt.Println(clusterStr)
	if do {
		for idx, shard :=range cluster.Shards {
			shardParam := handlers.CreateShardParam {
				Master: &shard.Nodes[0],
			}
			if len(shard.Nodes) > 1 {
				shardParam.Slaves = shard.Nodes[1:]
			}
			resp, err := util.HttpPost(handlers.GetShardRootURL(ctx.Leader, ctx.Namespace, ctx.Cluster), shardParam, 5 * time.Second)
			if err != nil {
				fmt.Println("create shard ", idx, " error: " + err.Error())
				return 
			}
			if resp.Errno != handlers.Success {
				fmt.Println("create shard  ", idx, " error: " + resp.Errmsg)	
				return
			}
			if resp.Body == nil {
				fmt.Println("create shard  ", idx, "error")
				return
			}
			fmt.Println("crate shard: ", idx, "response: ", resp.Body.(string))
		}
	} else {
		fmt.Println("add -d param, do above make shard plan")
	}
	return 
}

var DelShardCommand = cli.Command{
	Name:      "delshard",
	Usage:     "del shard",
	ArgsUsage: "-i ${shard_idx}",
	Action:    delShardAction,
	Flags: []cli.Flag{
		cli.IntFlag{
			Name:  "i,shardidx", 
			Value: -1, 
			Usage: "shard number"},
	},
	Description: `
    del shard under special cluster
    `,
}

func delShardAction(c *cli.Context) {
	ctx := cliCtx.GetContext()
	if ctx.Location != cliCtx.LocationCluster {
		fmt.Println("mkcl command should under clsuter dir")
		return 
	}
	shardIdx := c.Int("i")
	if shardIdx < 0 {
		fmt.Println("shard_idx(-i) error")
		return
	}
	resp, err := util.HttpDelete(handlers.GetShardURL(ctx.Leader, ctx.Namespace, ctx.Cluster, shardIdx), nil, 5 * time.Second)
	if err != nil {
		fmt.Println("delete shard error: " + err.Error())
		return 
	}
	if resp.Errno != handlers.Success {
		fmt.Println("create shard error: " + resp.Errmsg)	
		return
	}
	if resp.Body == nil {
		fmt.Println("create shard error")
		return
	}
	fmt.Println(resp.Body.(string))
}
