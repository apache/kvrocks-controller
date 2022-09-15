package handlers

import (
	"github.com/KvrocksLabs/kvrocks_controller/consts"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"github.com/gin-gonic/gin"
)

func Leader(c *gin.Context) {
	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	util.ResponseOK(c, stor.Leader())
}

func LeaderResign(c *gin.Context) {
	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	stor.Stop()
	util.ResponseOK(c, "OK")
}
