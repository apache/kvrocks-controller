package client

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/KvrocksLabs/kvrocks_controller/metadata"
)

func (c *Client) CreateClusterNode(ctx context.Context, namespace, cluster string,
	shardIndex int, nodeInfo *metadata.NodeInfo) error {
	bytes, _ := json.Marshal(nodeInfo)
	rsp, err := c.restyCli.R().SetContext(ctx).SetBody(bytes).
		SetPathParams(map[string]string{
			"namespace": namespace,
			"cluster":   cluster,
			"shard":     strconv.Itoa(shardIndex),
		}).Post("/api/v1/namespaces/{namespace}/clusters/{cluster}/shards/{shard}/nodes")
	if err != nil {
		return err
	}
	var status string
	return GetResponseData(rsp, &status)
}
