package client

import (
	"context"

	"github.com/KvrocksLabs/kvrocks_controller/storage"
)

func (c *Client) ListFailOverTask(ctx context.Context, namespace, cluster, typ string) ([]*storage.FailOverTask, error) {
	rsp, err := c.restyCli.R().SetContext(ctx).
		SetPathParams(map[string]string{
			"namespace": namespace,
			"cluster":   cluster,
			"type":      typ,
		}).Get("/api/v1/namespaces/{namespace}/clusters/{cluster}/failover/{type}")
	if err != nil {
		return nil, err
	}
	var tasks []*storage.FailOverTask
	if err := GetResponseData(rsp, &tasks); err != nil {
		return nil, err
	}
	return tasks, nil
}

func (c *Client) ListMigrationTask(ctx context.Context, namespace, cluster, typ string) ([]*storage.FailOverTask, error) {
	rsp, err := c.restyCli.R().SetContext(ctx).
		SetPathParams(map[string]string{
			"namespace": namespace,
			"cluster":   cluster,
			"type":      typ,
		}).Get("/api/v1/namespaces/{namespace}/clusters/{cluster}/migration/{type}")
	if err != nil {
		return nil, err
	}
	var tasks []*storage.FailOverTask
	if err := GetResponseData(rsp, &tasks); err != nil {
		return nil, err
	}
	return tasks, nil
}
