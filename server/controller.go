package server

import (
	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/storage"
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
