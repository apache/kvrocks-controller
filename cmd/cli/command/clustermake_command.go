package command

import (
	"fmt"
	"os"
	"bufio"
	"sort"
	"time"
	"context"
	"strings"
	"crypto/sha1"
    "encoding/hex"
    "strconv"

	"gopkg.in/urfave/cli.v1"
	rocks "github.com/go-redis/redis/v8"
	cliCtx "github.com/KvrocksLabs/kvrocks-controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks-controller/util"
	"github.com/KvrocksLabs/kvrocks-controller/server/handlers"
	"github.com/KvrocksLabs/kvrocks-controller/metadata"
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
	RESERVE_PORT  = 10000
)

func mkclAction(c *cli.Context) {
	ctx := cliCtx.GetContext()
	if ctx.Location != cliCtx.LocationNamespace {
		fmt.Println("mkcl command should under namespace dir")
		return 
	}
	clusterName := c.String("cn")
	shard       := c.Int("s")
	conf        := c.String("c")
	addrs       := c.String("n")
	do 	        := c.Bool("d")
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
	cluster := GenerateCluster(nodes, shard, true)
	if cluster == nil {
		return 
	}
	clusterStr, err := cluster.ToSlotString()
	if err != nil {
		fmt.Println("cluster to string error: ", err)
		return
	}
	fmt.Println("make cluster plan:")
	fmt.Println(clusterStr)
	if do {
		param := handlers.CreateClusterParam{
			Cluster: clusterName,
		}
		for _, shard :=range cluster.Shards {
			shardParam := handlers.CreateShardParam {
				Master: &shard.Nodes[0],
			}
			if len(shard.Nodes) > 1 {
				shardParam.Slaves = shard.Nodes[1:]
			}
			param.Shards = append(param.Shards, shardParam)
		}
		resp, err := util.HttpPost(handlers.GetClusterRootURL(ctx.Leader, ctx.Namespace), param, 5 * time.Second)
		if err != nil {
			fmt.Println("create cluster error: " + err.Error())
			return 
		}
		if resp.Errno != handlers.Success {
			fmt.Println("create cluster  error: " + resp.Errmsg)	
			return
		}
		if resp.Body == nil {
			fmt.Println("create cluster error")
			return
		}
		fmt.Println(resp.Body.(string))
	} else {
		fmt.Println("add -d param, do above make cluster plan")
	}
	return 
}

func GenerateCluster(nodes []string, shardNum int, assginShard bool) *metadata.Cluster {
	info := make(map[string][]string)
	for _, node := range nodes {
		addr := strings.Split(node, ":")
		if len(addr) != 2 {
			fmt.Println("node addr format err : ", node)
	    	return nil
		}
		if port, _ := strconv.Atoi(addr[1]); port >= (65535 - RESERVE_PORT) {
			fmt.Println("node port format more than (65535 - 10000) : ", node)
	    	return nil
		}
		client := rocks.NewClient(&rocks.Options{
	        Addr: node,
	    })
	    _, err := client.Do(context.Background(), "ping").Result()
	    if err != nil {
	    	fmt.Println("node: ", node, " ping err: ", err)
	    	return nil
	    }
		info[addr[0]] = append(info[addr[0]], addr[1])
	}

	cluster := &metadata.Cluster{}
	var ipInfo []string
	for ip, _ := range info {
		ipInfo = append(ipInfo, ip)
	}
	sort.Strings(ipInfo)

	slots := metadata.SpiltSlotRange(shardNum)
	// for master
	idx := 0
	for idx < shardNum {
		for _, ip := range ipInfo {
			if _, ok := info[ip]; !ok {
				continue
			}
			addr := ip + ":" + info[ip][0]
			c := sha1.New()
			c.Write([]byte(addr))
			bytes := c.Sum(nil)
			nodeID := hex.EncodeToString(bytes)

			shard := metadata.Shard{}
			shard.Nodes = append(shard.Nodes, metadata.NodeInfo{
					ID:        nodeID,
					CreatedAt: time.Now().Unix(),
					Address:   addr,
					Role:      metadata.RoleMaster,
			})
			if assginShard {
				shard.SlotRanges = append(shard.SlotRanges, slots[idx])
			}
			cluster.Shards = append(cluster.Shards, shard)
			if len(info[ip]) == 1 {
				delete(info, ip)
			} else {
				info[ip] = info[ip][1:]
			}
			idx++
			if idx == shardNum {
				break
			}
		}
	}
	// for slaves
	slaveNm := len(nodes) / shardNum - 1
	for slaveNm > 0 {
		for i, shard := range cluster.Shards {
			if len(shard.Nodes) == 0 {
				fmt.Println("assgin master err")
	    		return nil
			}
rotate:
			assgin := false
			for _, ip := range ipInfo {
				if _, ok := info[ip]; !ok {
					continue
				}
				masterAddr := strings.Split(shard.Nodes[0].Address, ":")
				if ip == masterAddr[0] && len(info) > 1 {
					continue
				} else {
					addr := ip + ":" + info[ip][0]
					c := sha1.New()
					c.Write([]byte(addr))
					bytes := c.Sum(nil)
					nodeID := hex.EncodeToString(bytes)
					slave := metadata.NodeInfo{
							ID:        nodeID,
							CreatedAt: time.Now().Unix(),
							Address:   addr,
							Role:      metadata.RoleSlave,
					}
					cluster.Shards[i].Nodes = append(cluster.Shards[i].Nodes, slave)
					if len(info[ip]) == 1 {
						delete(info, ip)
					} else {
						info[ip] = info[ip][1:]
					}
					assgin = true
					break
				}
			}
			if !assgin {
				goto rotate
			}
		}
		slaveNm--
	}
	return cluster
}
