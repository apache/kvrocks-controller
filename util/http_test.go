package util

import (
	"fmt"
	"time"
	"testing"

	"github.com/KvrocksLabs/kvrocks-controller/server/handlers"
	"github.com/KvrocksLabs/kvrocks-controller/metadata"
)

func GetCreateClusterParam() handlers.CreateClusterParam {
	return handlers.CreateClusterParam {
		Cluster: "testCluster",
		Shards: []handlers.CreateShardParam {
			handlers.CreateShardParam{
				Master: &metadata.NodeInfo{
							ID: 		"2bcefa7dff0aed57cacbce90134434587a10c891",
							CreatedAt:  time.Now().Unix(),
							Address: 	"127.0.0.1:6121",
							Role: 		metadata.RoleMaster,
							RequirePassword: "password",
							MasterAuth: 	 "auth",
						},
				Slaves: []metadata.NodeInfo{
							metadata.NodeInfo{
								ID: 		"75d76824d2e903af52b8c356941908132fef6b9f",
								CreatedAt:  time.Now().Unix(),
								Address: 	"127.0.0.1:6122",
								Role: 		metadata.RoleSlave,
								RequirePassword: "password",
								MasterAuth: 	 "auth",
							},
						},
			},
			handlers.CreateShardParam{
				Master: &metadata.NodeInfo{
							ID: 		"415cb13e439236d0fec257883e8ae1eacaa42244",
							CreatedAt:  time.Now().Unix(),
							Address: 	"127.0.0.1:6123",
							Role: 		metadata.RoleMaster,
							RequirePassword: "password",
							MasterAuth: 	 "auth",
						},
				Slaves: []metadata.NodeInfo{
							metadata.NodeInfo{
								ID: 		"56941908132fef6b9f75d76824d2e903af52b8c3",
								CreatedAt:  time.Now().Unix(),
								Address: 	"127.0.0.1:6124",
								Role: 		metadata.RoleSlave,
								RequirePassword: "password",
								MasterAuth: 	 "auth",
							},
						},
			},
		},
	}
}

func GetCreateShardParam() *handlers.CreateShardParam {
	return &handlers.CreateShardParam {
				Master: &metadata.NodeInfo{
							ID: 		"cacbce90134434587a10c8912bcefa7dff0aed57",
							CreatedAt:  time.Now().Unix(),
							Address: 	"127.0.0.1:6125",
							Role: 		metadata.RoleMaster,
							RequirePassword: "password",
							MasterAuth: 	 "auth",
						},
				Slaves: []metadata.NodeInfo{
							metadata.NodeInfo{
								ID: 		"8c356941908175d76824d2e903af52b32fef6b9f",
								CreatedAt:  time.Now().Unix(),
								Address: 	"127.0.0.1:6126",
								Role: 		metadata.RoleSlave,
								RequirePassword: "password",
								MasterAuth: 	 "auth",
							},
						},
			}
}

func TestStorage_Http(t *testing.T) {
	resp, err := HttpGet("http://127.0.0.1:9379/api/v1/controller/leader", nil, 0)
	fmt.Println("get controller id")
	fmt.Println(err)
	fmt.Println(resp)


	resp, err = HttpPost("http://127.0.0.1:9379/api/v1/namespaces", handlers.CreateNamespaceParam{Namespace: "testNs",}, 0)
	fmt.Println("create namespace")
	fmt.Println(err)
	fmt.Println(resp)

	resp, err = HttpGet("http://127.0.0.1:9379/api/v1/namespaces", nil, 0)
	fmt.Println("list namespace")
	fmt.Println(err)
	fmt.Println(resp)

	resp, err = HttpPost("http://127.0.0.1:9379/api/v1/namespaces/testNs/clusters", GetCreateClusterParam(), 0)
	fmt.Println("create cluster")
	fmt.Println(err)
	fmt.Println(resp)

	resp, err = HttpGet("http://127.0.0.1:9379/api/v1/namespaces/testNs/clusters/testCluster", nil, 0)
	fmt.Println("get cluster")
	fmt.Println(err)
	fmt.Println(resp)

	resp, err = HttpGet("http://127.0.0.1:9379/api/v1/namespaces/testNs/clusters", nil, 0)
	fmt.Println("list cluster")
	fmt.Println(err)
	fmt.Println(resp)
	
	resp, err := HttpPost("http://127.0.0.1:9379/api/v1/namespaces/testNs/clusters/testCluster/shards", GetCreateShardParam(), 0)
	fmt.Println("create shard")
	fmt.Println(err)
	fmt.Println(resp)

	resp, err = HttpGet("http://127.0.0.1:9379/api/v1/namespaces/testNs/clusters/testCluster/shards/0", nil, 0)
	fmt.Println("get shard")
	fmt.Println(err)
	fmt.Println(resp)

	resp, err = HttpGet("http://127.0.0.1:9379/api/v1/namespaces/testNs/clusters/testCluster/shards", nil, 0)
	fmt.Println("list shard")
	fmt.Println(err)
	fmt.Println(resp)

	resp, err := HttpDelete("http://127.0.0.1:9379/api/v1/namespaces/testNs/clusters/testCluster/shards/2", nil, 0)
	fmt.Println("delete shard")
	fmt.Println(err)
	fmt.Println(resp)

	slots := &handlers.ShardSlotsParam{
		Slots: []string{"0-100",},
	}
	resp, err = HttpDelete("http://127.0.0.1:9379/api/v1/namespaces/testNs/clusters/testCluster/shards/0/slots", slots, 0)
	fmt.Println("delete slots")
	fmt.Println(err)
	fmt.Println(resp)

	resp, err = HttpPost("http://127.0.0.1:9379/api/v1/namespaces/testNs/clusters/testCluster/shards/1/slots", slots, 0)
	fmt.Println("add slots")
	fmt.Println(err)
	fmt.Println(resp)

	node := &metadata.NodeInfo{
		ID: 		"f52b32fef6b9f8c356941908175d76824d2e903a",
		CreatedAt:  time.Now().Unix(),
		Address: 	"127.0.0.1:6128",
		Role: 		metadata.RoleSlave,
		RequirePassword: "password",
		MasterAuth: 	 "auth",
	}
	resp, err := HttpPost("http://127.0.0.1:9379/api/v1/namespaces/testNs/clusters/testCluster/shards/0/nodes", node, 0)
	fmt.Println("create nodes")
	fmt.Println(err)
	fmt.Println(resp)

	resp, err = HttpGet("http://127.0.0.1:9379/api/v1/namespaces/testNs/clusters/testCluster/shards/0/nodes", nil, 0)
	fmt.Println("list nodes")
	fmt.Println(err)
	fmt.Println(resp)

	resp, err := HttpDelete("http://127.0.0.1:9379/api/v1/namespaces/testNs/clusters/testCluster/shards/0/nodes/f52b32fef6b9f8c356941908175d76824d2e903a", nil, 0)
	fmt.Println("delete nodes")
	fmt.Println(err)
	fmt.Println(resp)

	resp, err  = HttpDelete("http://127.0.0.1:9379/api/v1/namespaces/testNs/clusters/testCluster", nil, 0)
	fmt.Println("delete cluster")
	fmt.Println(err)
	fmt.Println(resp)

	resp, err  = HttpDelete("http://127.0.0.1:9379/api/v1/namespaces/testNs", nil, 0)
	fmt.Println("delete namespace")
	fmt.Println(err)
	fmt.Println(resp)
}