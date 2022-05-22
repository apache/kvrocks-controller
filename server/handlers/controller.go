package handlers

import (
	"net/http"

	"github.com/KvrocksLabs/kvrocks-controller/consts"
	"github.com/KvrocksLabs/kvrocks-controller/storage"
	"github.com/KvrocksLabs/kvrocks-controller/util"
	"github.com/gin-gonic/gin"
)

func Leader(c *gin.Context) {
	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	c.JSON(http.StatusOK, util.MakeSuccessResponse(stor.Leader()))
}

func LeaderResign(c *gin.Context) {
	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	stor.LeaderResign()
	c.JSON(http.StatusOK, util.MakeSuccessResponse("OK"))
}