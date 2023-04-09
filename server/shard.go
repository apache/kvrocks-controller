package server

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/KvrocksLabs/kvrocks_controller/util"

	"github.com/KvrocksLabs/kvrocks_controller/consts"
	"github.com/KvrocksLabs/kvrocks_controller/controller/migrate"
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/gin-gonic/gin"
)

type ShardHandler struct {
	storage *storage.Storage
}

type SlotsRequest struct {
	Slots []string `json:"slots" validate:"required"`
}

type MigrateSlotDataRequest struct {
	Tasks []*storage.MigrateTask `json:"tasks" validate:"required"`
}

type MigrateSlotOnlyRequest struct {
	Source int                  `json:"source" validate:"required"`
	Target int                  `json:"target" validate:"required"`
	Slots  []metadata.SlotRange `json:"slots" validate:"required"`
}

type CreateShardRequest struct {
	Master *metadata.NodeInfo  `json:"master"`
	Slaves []metadata.NodeInfo `json:"slaves"`
}

func (handler *ShardHandler) List(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")

	shards, err := handler.storage.ListShard(c, ns, cluster)
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, gin.H{"shards": shards})
}

func (handler *ShardHandler) Get(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err)
		return
	}

	s, err := handler.storage.GetShard(c, ns, cluster, shard)
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, gin.H{"shard": s})
}

func (handler *ShardHandler) Create(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")

	var req struct {
		Nodes []string `json:"nodes"`
	}
	if err := c.BindJSON(&req); err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err)
		return
	}
	if len(req.Nodes) == 0 {
		responseErrorWithCode(c, http.StatusBadRequest, errors.New("nodes should NOT be empty"))
		return
	}
	nodes := make([]metadata.NodeInfo, len(req.Nodes))
	for i, nodeAddr := range req.Nodes {
		nodes[i].ID = util.GenerateNodeID()
		nodes[i].Address = nodeAddr
		if i == 0 {
			nodes[i].Role = metadata.RoleMaster
		} else {
			nodes[i].Role = metadata.RoleSlave
		}
	}
	if err := handler.storage.CreateShard(c, ns, cluster, &metadata.Shard{
		Nodes:         nodes,
		ImportSlot:    -1,
		MigratingSlot: -1,
	}); err != nil {
		responseError(c, err)
		return
	}
	// TODO: return shard id
	responseCreated(c, "OK")
}

func (handler *ShardHandler) Remove(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err)
		return
	}

	err = handler.storage.RemoveShard(c, ns, cluster, shard)
	if err != nil {
		responseError(c, err)
		return
	}
	response(c, http.StatusNoContent, nil)
}

func (handler *ShardHandler) UpdateSlots(c *gin.Context) {
	isAdd := c.Request.Method == http.MethodPost
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err)
		return
	}
	var payload SlotsRequest
	if err := c.BindJSON(&payload); err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err)
		return
	}
	slotRanges := make([]metadata.SlotRange, len(payload.Slots))
	for i, slot := range payload.Slots {
		slotRange, err := metadata.ParseSlotRange(slot)
		if err != nil {
			responseErrorWithCode(c, http.StatusBadRequest, err)
			return
		}
		slotRanges[i] = *slotRange
	}

	storage := handler.storage
	if isAdd {
		err = storage.AddShardSlots(c, ns, cluster, shard, slotRanges)
	} else {
		err = storage.RemoveShardSlots(c, ns, cluster, shard, slotRanges)
	}
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, "OK")
}

func (handler *ShardHandler) MigrateSlotData(c *gin.Context) {
	var req MigrateSlotDataRequest
	if err := c.BindJSON(&req); err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err)
		return
	}
	migration := c.MustGet(consts.ContextKeyMigrate).(*migrate.Migrate)
	if err := migration.AddTasks(c, req.Tasks); err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, "OK")
}

func (handler *ShardHandler) MigrateSlotOnly(c *gin.Context) {
	var req MigrateSlotOnlyRequest
	if err := c.BindJSON(&req); err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err)
		return
	}
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	if err := handler.storage.RemoveShardSlots(c, ns, cluster, req.Source, req.Slots); err != nil {
		responseError(c, err)
		return
	}
	if err := handler.storage.AddShardSlots(c, ns, cluster, req.Target, req.Slots); err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, "OK")
}
