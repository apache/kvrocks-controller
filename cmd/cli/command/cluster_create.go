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

var CreateClusterCommand = cli.Command{
	Name:      "create_cluster",
	Usage:     "Create a new cluster",
	ArgsUsage: "-c ${cluster_name} -s ${shard_count} -n ${node_addr1,node_addr2...}/-c ${config_path} -e ${execute}",
	Action:    createCluster,
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "c,cluster_name",
			Value: "",
			Usage: "Cluster name"},
		cli.IntFlag{
			Name:  "s,shard",
			Value: 1,
			Usage: "Shard count"},
		cli.StringFlag{
			Name:  "n,nodes",
			Value: "",
			Usage: "Kvrocks node address"},
		cli.StringFlag{
			Name:  "c,config",
			Value: "",
			Usage: "config path"},
		cli.BoolFlag{
			Name:  "e,execute",
			Usage: "flag do init cluster"},
	},
	Description: "Create cluster under namespace",
}
var (
	ReservedPort = 10000
)

func createCluster(c *cli.Context) {
	ctx := context.GetContext()
	if ctx.Location != context.LocationNamespace {
		fmt.Println("You must enter an namespace before creating cluster")
		return
	}

	clusterName := c.String("c")
	shard := c.Int("s")
	conf := c.String("c")
	addrs := c.String("n")
	execute := c.Bool("e")
	if clusterName == "" {
		fmt.Println("Cluster name cannot be empty")
		return
	}
	if strings.Contains(clusterName, "/") {
		fmt.Println("cluster name can't contain '/'")
		return
	}
	if len(conf) != 0 && len(addrs) != 0 {
		fmt.Println("`-c` or `-n` cannot be assigned at the same time")
		return
	}
	if conf == "" && addrs == "" {
		fmt.Println("required `-c` or `-n` when creating the new cluster")
		return
	}

	var nodes []string
	if conf != "" {
		file, err := os.Open(conf)
		if err != nil {
			fmt.Println("Can't open config file err: ", err)
			return
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			nodes = append(nodes, line)
		}
		if err := scanner.Err(); err != nil {
			fmt.Println("Scan config file err: ", err)
			return
		}
	} else {
		nodes = strings.Split(addrs, ",")
	}

	// check node and shard parameter
	nodeSize := len(nodes)
	if nodeSize == 0 {
		fmt.Println("No node was found")
		return
	}
	if nodeSize < shard {
		fmt.Println("The node number should be greater than the shard number")
		return
	}
	if nodeSize%shard != 0 {
		fmt.Println("The node number can't divide shard number")
		return
	}
	sort.Strings(nodes)

	cluster := GenerateCluster(nodes, shard, true)
	if cluster == nil {
		return
	}
	if !visableCluster(cluster) {
		return
	}

	if execute {
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
		HttpResponeException("Create cluster", resp, err)
	} else {
		fmt.Println("Please add `-e` if you want to execute the create cluster command")
	}
}
