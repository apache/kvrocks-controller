package command

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/storage/persistence/etcd"
	"github.com/KvrocksLabs/kvrocks_controller/util"
)

func responseError(title string, resp *util.Response, err error) bool {
	errPrefix := title + " error "
	if err != nil {
		fmt.Println(errPrefix + err.Error())
		return true
	}
	if resp.Error != nil {
		fmt.Println(errPrefix + resp.Error.Message)
		return true
	}
	if resp.Data == nil {
		fmt.Println(errPrefix + "response is nil")
		return true
	}
	if resStr, ok := resp.Data.(string); ok {
		fmt.Println(resStr)
	}
	return false
}

func getStringList(names interface{}) []string {
	var res []string
	objs := reflect.ValueOf(names)
	for i := 0; i < objs.Len(); i++ {
		obj := objs.Index(i)
		res = append(res, obj.Interface().(string))
	}
	return res
}

func showStringList(names []string, title string) {
	fmt.Println(title + ":")
	for _, name := range names {
		fmt.Printf("\t%s\n", name)
	}
}

func accessNodeID(addr string) string {
	sha := sha1.New()
	sha.Write([]byte(addr))
	bytes := sha.Sum(nil)
	return hex.EncodeToString(bytes)
}

func visableTask(task *etcd.MigrateTask) bool {
	taskData, err := json.Marshal(*task)
	if err != nil {
		fmt.Println("migrate task error: " + err.Error())
		return false
	}

	var out bytes.Buffer
	err = json.Indent(&out, taskData, "", "\t")
	if err != nil {
		fmt.Println("migrate task format error: " + err.Error())
		return false
	}

	out.WriteTo(os.Stdout)
	fmt.Printf("\n")
	return true
}

func visableCluster(cluster *metadata.Cluster) bool {
	clusterStr, err := cluster.ToSlotString()
	if err != nil {
		fmt.Println("cluster to string error: ", err)
		return false
	}
	fmt.Println("make cluster plan:")
	fmt.Println(clusterStr)
	return true
}

func nodesToInterfaceSlice(nodes []*ShowNode) []interface{} {
	var interfaceSlice []interface{} = make([]interface{}, len(nodes))
	for i, node := range nodes {
		interfaceSlice[i] = node
	}
	return interfaceSlice
}

func failtasksToInterfaceSlice(nodes []*FailTask) []interface{} {
	var interfaceSlice []interface{} = make([]interface{}, len(nodes))
	for i, node := range nodes {
		interfaceSlice[i] = node
	}
	return interfaceSlice
}

func migratetasksToInterfaceSlice(nodes []*MigTask) []interface{} {
	var interfaceSlice []interface{} = make([]interface{}, len(nodes))
	for i, node := range nodes {
		interfaceSlice[i] = node
	}
	return interfaceSlice
}

func GenerateCluster(nodes []string, shardNum int, assginShard bool) *metadata.Cluster {
	info := make(map[string][]string)
	for _, node := range nodes {
		addr := strings.Split(node, ":")
		if len(addr) != 2 {
			fmt.Println("node addr format err : ", node)
			return nil
		}
		if port, _ := strconv.Atoi(addr[1]); port >= (65535 - ReservedPort) {
			fmt.Println("node port format more than (65535 - 10000) : ", node)
			return nil
		}
		client, err := util.NewRedisClient(node)
		if err != nil {
			fmt.Printf("addr: %s, dail error : %s\n", node, err.Error())
			return nil
		}
		_, err = client.Do(context.Background(), "ping").Result()
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
			shard := metadata.Shard{}
			shard.Nodes = append(shard.Nodes, metadata.NodeInfo{
				ID:        accessNodeID(addr),
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
	slaveNm := len(nodes)/shardNum - 1
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
