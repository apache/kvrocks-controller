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
			util.ResponseErrorWithCode(c, http.StatusNotFound, err.Error())
		} else {
			util.ResponseError(c, err.Error())
		}
		return
	}
	util.ResponseOK(c, namespaces)
}

func CreateNamespace(c *gin.Context) {
	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	param := CreateNamespaceParam{}
	if err := c.BindJSON(&param); err != nil {
		util.ResponseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}
	if len(param.Namespace) == 0 {
		util.ResponseErrorWithCode(c, http.StatusConflict, "namespace should NOT be empty")
		return
	}

	if err := stor.CreateNamespace(param.Namespace); err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeExisted {
			util.ResponseErrorWithCode(c, http.StatusConflict, err.Error())
		} else {
			util.ResponseError(c, err.Error())
		}
		return
	}
	util.ResponseOK(c, "OK")
}

func RemoveNamespace(c *gin.Context) {
	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	namespace := c.Param("namespace")
	if err := stor.RemoveNamespace(namespace); err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeNoExists {
			util.ResponseErrorWithCode(c, http.StatusNotFound, err.Error())
		} else {
			util.ResponseError(c, err.Error())
		}
		return
	}
	util.ResponseOK(c, "OK")
}
