package handlers

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/KvrocksLabs/kvrocks-controller/consts"
	"github.com/KvrocksLabs/kvrocks-controller/metadata"
	"github.com/KvrocksLabs/kvrocks-controller/storage"
	"github.com/gin-gonic/gin"
)

func (req *CreateShardParam) validate() error {
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

func (req *CreateShardParam) toShard() (*metadata.Shard, error) {
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

func ListShard(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")

	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	shards, err := stor.ListShard(ns, cluster)
	if err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeNoExists {
			c.JSON(http.StatusNotFound, MakeFailureResponse(err.Error()))
		} else {
			c.JSON(http.StatusInternalServerError, MakeFailureResponse(err.Error()))
		}
		return
	}
	c.JSON(http.StatusOK, MakeSuccessResponse(shards))
}

func GetShard(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		c.JSON(http.StatusBadRequest, MakeFailureResponse(err.Error()))
		return
	}

	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	s, err := stor.GetShard(ns, cluster, shard)
	if err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeNoExists {
			c.JSON(http.StatusNotFound, MakeFailureResponse(err.Error()))
		} else {
			c.JSON(http.StatusInternalServerError, MakeFailureResponse(err.Error()))
		}
		return
	}
	c.JSON(http.StatusOK, MakeSuccessResponse(s))
}

func CreateShard(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")

	var req CreateShardParam
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, MakeFailureResponse(err.Error()))
		return
	}
	shard, err := req.toShard()
	if err != nil {
		c.JSON(http.StatusBadRequest, MakeFailureResponse(err.Error()))
		return
	}

	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	if err := stor.CreateShard(ns, cluster, shard); err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeExisted {
			c.JSON(http.StatusConflict, MakeFailureResponse(err.Error()))
		} else {
			c.JSON(http.StatusInternalServerError, MakeFailureResponse(err.Error()))
		}
		return
	}
	c.JSON(http.StatusCreated, MakeSuccessResponse("OK"))
}

func RemoveShard(c *gin.Context) {
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		c.JSON(http.StatusBadRequest, MakeFailureResponse(err.Error()))
		return
	}

	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	if err := stor.RemoveShard(ns, cluster, shard); err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeNoExists {
			c.JSON(http.StatusNotFound, MakeFailureResponse(err.Error()))
		} else {
			c.JSON(http.StatusInternalServerError, MakeFailureResponse(err.Error()))
		}
		return
	}
	c.JSON(http.StatusCreated, MakeSuccessResponse("OK"))
}

func UpdateShardSlots(c *gin.Context) {
	isAdd := c.Request.Method == http.MethodPost
	ns := c.Param("namespace")
	cluster := c.Param("cluster")
	shard, err := strconv.Atoi(c.Param("shard"))
	if err != nil {
		c.JSON(http.StatusBadRequest, MakeFailureResponse(err.Error()))
		return
	}
	var payload ShardSlotsParam
	if err := c.BindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, MakeFailureResponse(err.Error()))
		return
	}
	slotRanges := make([]metadata.SlotRange, len(payload.Slots))
	for i, slot := range payload.Slots {
		slotRange, err := metadata.ParseSlotRange(slot)
		if err != nil {
			c.JSON(http.StatusBadRequest, MakeFailureResponse(err.Error()))
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
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeNoExists {
			c.JSON(http.StatusNotFound, MakeFailureResponse(err.Error()))
		} else {
			c.JSON(http.StatusInternalServerError, MakeFailureResponse(err.Error()))
		}
		return
	}
	c.JSON(http.StatusOK, MakeSuccessResponse("OK"))
}
