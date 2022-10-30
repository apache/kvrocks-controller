package handlers

import (
	"errors"
	"fmt"

	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/storage/persistence/etcd"
)

type CreateClusterRequest struct {
	Cluster string               `json:"cluster"`
	Shards  []CreateShardRequest `json:"shards"`
}

func (req *CreateClusterRequest) validate() error {
	if len(req.Cluster) == 0 {
		return fmt.Errorf("cluster name should NOT be empty")
	}
	for i, shard := range req.Shards {
		if err := shard.validate(); err != nil {
			return fmt.Errorf("validate shard[%d] err: %w", i, err)
		}
	}
	return nil
}

type CreateShardRequest struct {
	Master *metadata.NodeInfo  `json:"master"`
	Slaves []metadata.NodeInfo `json:"slaves"`
}

func (req *CreateShardRequest) validate() error {
	if req.Master == nil {
		return errors.New("missing master node")
	}

	req.Master.Role = metadata.RoleMaster
	if err := req.Master.Validate(); err != nil {
		return err
	}
	if len(req.Slaves) > 0 {
		for i := range req.Slaves {
			req.Slaves[i].Role = metadata.RoleSlave
			if err := req.Slaves[i].Validate(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (req *CreateShardRequest) toShard() (*metadata.Shard, error) {
	if err := req.validate(); err != nil {
		return nil, err
	}

	shard := metadata.NewShard()
	shard.Nodes = append(shard.Nodes, *req.Master)
	if len(req.Slaves) > 0 {
		shard.Nodes = append(shard.Nodes, req.Slaves...)
	}

	return shard, nil
}

type SlotsRequest struct {
	Slots []string `json:"slots" validate:"required"`
}

type MigrateSlotDataRequest struct {
	Tasks []*etcd.MigrateTask `json:"tasks" validate:"required"`
}

type MigrateSlotOnlyRequest struct {
	Source int                  `json:"source" validate:"required"`
	Target int                  `json:"target" validate:"required"`
	Slots  []metadata.SlotRange `json:"slots" validate:"required"`
}
