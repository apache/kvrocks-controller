package client

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/KvrocksLabs/kvrocks_controller/server"

	"github.com/KvrocksLabs/kvrocks_controller/metadata"
)

func (c *Client) ListCluster(ctx context.Context, namespace string) ([]string, error) {
	rsp, err := c.restyCli.R().SetContext(ctx).
		SetPathParam("namespace", namespace).
		Get("/api/v1/namespaces/{namespace}/clusters")
	if err != nil {
		return nil, err
	}
	clusters := make([]string, 0)
	if err := GetResponseData(rsp, &clusters); err != nil {
		return nil, err
	}
	return clusters, nil
}

func (c *Client) CreateCluster(ctx context.Context, namespace string, req *server.CreateClusterRequest) error {
	bytes, _ := json.Marshal(req) // nolint
	rsp, err := c.restyCli.R().SetContext(ctx).
		SetPathParam("namespace", namespace).SetBody(bytes).
		Post("/api/v1/namespaces/{namespace}/clusters")
	if err != nil {
		return err
	}
	var status string
	return GetResponseData(rsp, &status)
}

func (c *Client) RemoveCluster(ctx context.Context, namespace, cluster string) error {
	rsp, err := c.restyCli.R().SetContext(ctx).
		SetPathParam("namespace", namespace).
		SetPathParam("cluster", cluster).
		Delete("/api/v1/namespaces/{namespace}/clusters/{cluster}")
	if err != nil {
		return err
	}
	var status string
	return GetResponseData(rsp, &status)
}

func (c *Client) GetCluster(ctx context.Context, namespace, cluster string) (*metadata.Cluster, error) {
	rsp, err := c.restyCli.R().SetContext(ctx).
		SetPathParams(map[string]string{
			"namespace": namespace,
			"cluster":   cluster,
		}).Get("/api/v1/namespaces/{namespace}/clusters/{cluster}")
	if err != nil {
		return nil, err
	}
	var clusterInfo metadata.Cluster
	if err := GetResponseData(rsp, &clusterInfo); err != nil {
		return nil, err
	}
	return &clusterInfo, nil
}

func (c *Client) GetClusterShard(ctx context.Context, namespace, cluster string, shardIndex int) (*metadata.Shard, error) {
	rsp, err := c.restyCli.R().SetContext(ctx).
		SetPathParams(map[string]string{
			"namespace": namespace,
			"cluster":   cluster,
			"shard":     strconv.Itoa(shardIndex),
		}).Get("/api/v1/namespaces/{namespace}/clusters/{cluster}/shards/{shard}")
	if err != nil {
		return nil, err
	}
	var shard metadata.Shard
	if err := GetResponseData(rsp, &shard); err != nil {
		return nil, err
	}
	return &shard, nil
}

func (c *Client) CreateClusterShard(ctx context.Context,
	namespace, cluster string,
	req *server.CreateShardRequest) error {
	bytes, _ := json.Marshal(req) // nolint
	rsp, err := c.restyCli.R().SetContext(ctx).
		SetPathParams(map[string]string{
			"namespace": namespace,
			"cluster":   cluster,
		}).SetBody(bytes).
		Post("/api/v1/namespaces/{namespace}/clusters/{cluster}/shards")
	if err != nil {
		return err
	}
	var status string
	return GetResponseData(rsp, &status)
}

func (c *Client) RemoveClusterShard(ctx context.Context, namespace, cluster string, shardIndex int) error {
	rsp, err := c.restyCli.R().SetContext(ctx).
		SetPathParams(map[string]string{
			"namespace": namespace,
			"cluster":   cluster,
			"shard":     strconv.Itoa(shardIndex),
		}).Delete("/api/v1/namespaces/{namespace}/clusters/{cluster}/shards/{shard}")
	if err != nil {
		return err
	}
	var status string
	return GetResponseData(rsp, &status)
}
