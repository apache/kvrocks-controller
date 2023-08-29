package server

import (
	"github.com/RocksLabs/kvrocks_controller/consts"
	"github.com/RocksLabs/kvrocks_controller/storage"
	"github.com/gin-gonic/gin"
)

func Leader(c *gin.Context) {
	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	responseOK(c, stor.Leader())
}

func LeaderResign(c *gin.Context) {
	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	stor.Stop()
	responseOK(c, "OK")
}
