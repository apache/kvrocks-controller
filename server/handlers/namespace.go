package handlers

import (
	"net/http"

	"github.com/KvrocksLabs/kvrocks_controller/consts"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/gin-gonic/gin"
)

func ListNamespace(c *gin.Context) {
	storage := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	namespaces, err := storage.ListNamespace()
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, namespaces)
}

func CreateNamespace(c *gin.Context) {
	storage := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	var request struct {
		Namespace string `json:"namespace"`
	}
	if err := c.BindJSON(&request); err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}
	if len(request.Namespace) == 0 {
		responseErrorWithCode(c, http.StatusConflict, "namespace should NOT be empty")
		return
	}

	if err := storage.CreateNamespace(request.Namespace); err != nil {
		responseError(c, err)
		return
	}
	responseCreated(c, "Created")
}

func RemoveNamespace(c *gin.Context) {
	storage := c.MustGet(consts.ContextKeyStorage).(*storage.Storage)
	namespace := c.Param("namespace")
	if err := storage.RemoveNamespace(namespace); err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, "OK")
}
