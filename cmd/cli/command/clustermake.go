package command

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/KvrocksLabs/kvrocks_controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks_controller/server/handlers"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"gopkg.in/urfave/cli.v1"
)

var MkclCommand = cli.Command{
	Name:      "mkcl",
	Usage:     "make cluster",
	ArgsUsage: "-cn ${clustername} -s ${shard_number} -n ${nodeaddr1,nodeaddr2...}/-c ${configpath} -d ${do}",
	Action:    mkclAction,
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "cn,clustername",
			Value: "",
			Usage: "cluster name"},
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
    make cluster under special namespaces
    `,
}
var (
	RESERVE_PORT = 10000
)

func mkclAction(c *cli.Context) {
	ctx := context.GetContext()
	if ctx.Location != context.LocationNamespace {
		fmt.Println("mkcl command should under namespace dir")
		return
	}

	clusterName := c.String("cn")
	shard := c.Int("s")
	conf := c.String("c")
	addrs := c.String("n")
	do := c.Bool("d")
	if clusterName == "" {
		fmt.Println("clusterName(-cn) cannot be empty")
		return
	}
	if strings.Contains(clusterName, "/") {
		fmt.Println("cluster can't contain '/'")
		return
	}
	if len(conf) != 0 && len(addrs) != 0 {
		fmt.Println("config path(-c) or nodes address(-n), cannot be set at the same time")
		return
	}
	if conf == "" && addrs == "" {
		fmt.Println("config path(-c) or nodes address(-n), at least one be set")
		return
	}

	// parser and sort nodeaddr
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

	// check node and shard parameter
	nodeSize := len(nodes)
	if nodeSize == 0 {
		fmt.Println("nodes is empty")
		return
	}
	if nodeSize < shard {
		fmt.Println("nodes less shard number")
		return
	}
	if nodeSize%shard != 0 {
		fmt.Println("nodes can't divide shard number")
		return
	}
	sort.Strings(nodes)

	// init cluster plan and visualization
	cluster := GenerateCluster(nodes, shard, true)
	if cluster == nil {
		return
	}
	if !visableCluster(cluster) {
		return
	}

	// post request
	if do {
		param := handlers.CreateClusterParam{
			Cluster: clusterName,
		}
		for _, shard := range cluster.Shards {
			shardParam := handlers.CreateShardParam{
				Master: &shard.Nodes[0],
			}
			if len(shard.Nodes) > 1 {
				shardParam.Slaves = shard.Nodes[1:]
			}
			param.Shards = append(param.Shards, shardParam)
		}
		resp, err := util.HttpPost(handlers.GetClusterRootURL(ctx.Leader, ctx.Namespace), param, 5*time.Second)
		HttpResponeException("make cluster", resp, err)
	} else {
		fmt.Println("add -d param, do above make cluster plan")
	}
}
