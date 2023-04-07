package server

import (
	"net/http"
	"strconv"

	"github.com/KvrocksLabs/kvrocks_controller/consts"
	"github.com/KvrocksLabs/kvrocks_controller/controller/migrate"
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/gin-gonic/gin"
)

type ShardHandler struct {
	storage *storage.Storage
}

func (handler *ShardHandler) List(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")

	shards, err := handler.storage.ListShard(c, ns, cluster)
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, shards)
}

func (handler *ShardHandler) Get(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}

	s, err := handler.storage.GetShard(c, ns, cluster, shard)
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, s)
}

func (handler *ShardHandler) Create(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")

	var req CreateShardRequest
	if err := c.BindJSON(&req); err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}
	shard, err := req.toShard()
	if err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}

	if err := handler.storage.CreateShard(c, ns, cluster, shard); err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, "OK")
}

func (handler *ShardHandler) Remove(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}

	err = handler.storage.RemoveShard(c, ns, cluster, shard)
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, "OK")
}

func (handler *ShardHandler) UpdateSlots(c *gin.Context) {
	isAdd := c.Request.Method == http.MethodPost
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}
	var payload SlotsRequest
	if err := c.BindJSON(&payload); err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}
	slotRanges := make([]metadata.SlotRange, len(payload.Slots))
	for i, slot := range payload.Slots {
		slotRange, err := metadata.ParseSlotRange(slot)
		if err != nil {
			responseErrorWithCode(c, http.StatusBadRequest, err.Error())
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
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
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
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
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
