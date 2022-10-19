package handlers

import (
	"net/http"
	"strconv"

	"github.com/KvrocksLabs/kvrocks_controller/consts"
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/migrate"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/gin-gonic/gin"
)

func ListShard(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")

	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	shards, err := stor.ListShard(ns, cluster)
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, shards)
}

func GetShard(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}

	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	s, err := stor.GetShard(ns, cluster, shard)
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, s)
}

func CreateShard(c *gin.Context) {
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

	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	if err := stor.CreateShard(ns, cluster, shard); err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, "OK")
}

func RemoveShard(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}

	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	if err := stor.RemoveShard(ns, cluster, shard); err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, "OK")
}

func UpdateShardSlots(c *gin.Context) {
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

	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	if isAdd {
		err = stor.AddShardSlots(ns, cluster, shard, slotRanges)
	} else {
		err = stor.RemoveShardSlots(ns, cluster, shard, slotRanges)
	}
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, "OK")
}

func MigrateSlotData(c *gin.Context) {
	var req MigrateSlotDataRequest
	if err := c.BindJSON(&req); err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}
	migr := c.MustGet(consts.ContextKeyMigrate).(*migrate.Migrate)
	if err := migr.AddTasks(req.Tasks); err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, "OK")
}

func MigrateSlotOnly(c *gin.Context) {
	var req MigrateSlotOnlyRequest
	if err := c.BindJSON(&req); err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	if err := stor.RemoveShardSlots(ns, cluster, req.Source, req.Slots); err != nil {
		responseError(c, err)
		return
	}
	if err := stor.AddShardSlots(ns, cluster, req.Target, req.Slots); err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, "OK")
}
