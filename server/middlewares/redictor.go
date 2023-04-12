package middlewares

import (
	"net/http"

	"github.com/KvrocksLabs/kvrocks_controller/consts"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/gin-gonic/gin"
)

func RedirectIfNotLeader(c *gin.Context) {
	storage, _ := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	if !storage.IsLeader() && !c.GetBool(consts.HeaderIsRedirect) {
		c.Set(consts.HeaderIsRedirect, true)
		c.Redirect(http.StatusTemporaryRedirect, "http://"+storage.Leader()+c.Request.RequestURI)
		c.Abort()
		return
	}
}
