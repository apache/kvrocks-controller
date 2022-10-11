package handlers

import (
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/storage/persistence/etcd"
)

type CreateNamespaceRequest struct {
	Namespace string `json:"namespace"`
}

type CreateClusterRequest struct {
	Cluster string               `json:"cluster"`
	Shards  []CreateShardRequest `json:"shards"`
}

type CreateShardRequest struct {
	Master *metadata.NodeInfo  `json:"master"`
	Slaves []metadata.NodeInfo `json:"slaves"`
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
