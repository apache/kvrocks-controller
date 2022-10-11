package handlers

import (
	"net/http"

	"github.com/KvrocksLabs/kvrocks_controller/consts"
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/gin-gonic/gin"
)

func ListNamespace(c *gin.Context) {
	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	namespaces, err := stor.ListNamespace()
	if err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeNoExists {
			responseErrorWithCode(c, http.StatusNotFound, err.Error())
		} else {
			responseError(c, err.Error())
		}
		return
	}
	responseOK(c, namespaces)
}

func CreateNamespace(c *gin.Context) {
	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	param := CreateNamespaceRequest{}
	if err := c.BindJSON(&param); err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}
	if len(param.Namespace) == 0 {
		responseErrorWithCode(c, http.StatusConflict, "namespace should NOT be empty")
		return
	}

	if err := stor.CreateNamespace(param.Namespace); err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeExisted {
			responseErrorWithCode(c, http.StatusConflict, err.Error())
		} else {
			responseError(c, err.Error())
		}
		return
	}
	responseOK(c, "OK")
}

func RemoveNamespace(c *gin.Context) {
	stor := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	namespace := c.Param("namespace")
	if err := stor.RemoveNamespace(namespace); err != nil {
		if metaErr, ok := err.(*metadata.Error); ok && metaErr.Code == metadata.CodeNoExists {
			responseErrorWithCode(c, http.StatusNotFound, err.Error())
		} else {
			responseError(c, err.Error())
		}
		return
	}
	responseOK(c, "OK")
}
