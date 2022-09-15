package command

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/KvrocksLabs/kvrocks_controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks_controller/server/handlers"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"gopkg.in/urfave/cli.v1"
)

var AddShardCommand = cli.Command{
	Name:      "add_shard",
	Usage:     "Add shard",
	ArgsUsage: "-s ${shard} -n ${nodeaddr1,nodeaddr2...}/-c ${config} -e ${execute}",
	Action:    addShard,
	Flags: []cli.Flag{
		cli.IntFlag{
			Name:  "s,shard",
			Value: 1,
			Usage: "Shard index"},
		cli.StringFlag{
			Name:  "n,nodes",
			Value: "",
			Usage: "Kvrocks node addresses"},
		cli.StringFlag{
			Name:  "c,config",
			Value: "",
			Usage: "Config path"},
		cli.BoolFlag{
			Name:  "e,execute",
			Usage: "Execute the add shard action"},
	},
	Description: `
    Create a new shard in cluster 
    `,
}

func addShard(c *cli.Context) {
	ctx := context.GetContext()
	if ctx.Location != context.LocationCluster {
		fmt.Println("Command add_shard should be under cluster dir")
		return
	}
	shard := c.Int("s")
	conf := c.String("c")
	addrs := c.String("n")
	do := c.Bool("d")
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
		fmt.Println("No node was found")
		return
	}
	if nodeSize < shard {
		fmt.Println("The node number is less than the shard number")
		return
	}
	if nodeSize%shard != 0 {
		fmt.Println("The node number can't be divided by the shard number")
		return
	}
	sort.Strings(nodes)
	cluster := GenerateCluster(nodes, shard, false)
	if cluster == nil {
		return
	}
	clusterStr, err := cluster.ToSlotString()
	if err != nil {
		fmt.Println("Cluster to string error: ", err)
		return
	}
	fmt.Println("Add shard plan:")
	fmt.Println(clusterStr)
	if do {
		for idx, shard := range cluster.Shards {
			shardParam := handlers.CreateShardParam{
				Master: &shard.Nodes[0],
			}
			if len(shard.Nodes) > 1 {
				shardParam.Slaves = shard.Nodes[1:]
			}
			resp, err := util.HttpPost(handlers.GetShardRootURL(ctx.Leader, ctx.Namespace, ctx.Cluster), shardParam, 5*time.Second)
			if responseError("Create shard"+strconv.Itoa(idx), resp, err) {
				return
			}
			fmt.Println("Create shard", idx, "response: ", resp.Data)
		}
	} else {
		fmt.Println("add -e param to execute the above plan")
	}
}
