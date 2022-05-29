package handlers

import (
	"net/http"

	"github.com/KvrocksLabs/kvrocks_controller/consts"
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"github.com/gin-gonic/gin"
)

func ListNamespace(c *gin.Context) {
	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	namespaces, err := stor.ListNamespace()
	if err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeNoExists {
			c.JSON(http.StatusNotFound, util.MakeFailureResponse(err.Error()))
		} else {
			c.JSON(http.StatusInternalServerError, util.MakeFailureResponse(err.Error()))
		}
		return
	}
	c.JSON(http.StatusOK, util.MakeSuccessResponse(namespaces))
}

func CreateNamespace(c *gin.Context) {
	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	param := CreateNamespaceParam{}
	if err := c.BindJSON(&param); err != nil {
		c.JSON(http.StatusBadRequest, util.MakeFailureResponse(err.Error()))
		return
	}
	if len(param.Namespace) == 0 {
		c.JSON(http.StatusBadRequest, util.MakeFailureResponse("namespace should NOT be empty"))
		return
	}

	if err := stor.CreateNamespace(param.Namespace); err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeExisted {
			c.JSON(http.StatusConflict, util.MakeFailureResponse(err.Error()))
		} else {
			c.JSON(http.StatusInternalServerError, util.MakeFailureResponse(err.Error()))
		}
		return
	}
	c.JSON(http.StatusOK, util.MakeSuccessResponse("OK"))
}

func RemoveNamespace(c *gin.Context) {
	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	namespace := c.Param("namespace")
	if err := stor.RemoveNamespace(namespace); err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeNoExists {
			c.JSON(http.StatusNotFound, util.MakeFailureResponse(err.Error()))
		} else {
			c.JSON(http.StatusInternalServerError, util.MakeFailureResponse(err.Error()))
		}
		return
	}
	c.JSON(http.StatusOK, util.MakeSuccessResponse("OK"))
}
