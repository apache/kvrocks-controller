package server

import (
	"net/http"

	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/gin-gonic/gin"
)

type NamespaceHandler struct {
	storage *storage.Storage
}

func (handler *NamespaceHandler) List(c *gin.Context) {
	namespaces, err := handler.storage.ListNamespace(c)
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, namespaces)
}

func (handler *NamespaceHandler) Create(c *gin.Context) {
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

	if err := handler.storage.CreateNamespace(c, request.Namespace); err != nil {
		responseError(c, err)
		return
	}
	responseCreated(c, "Created")
}

func (handler *NamespaceHandler) Remove(c *gin.Context) {
	namespace := c.Param("namespace")
	if err := handler.storage.RemoveNamespace(c, namespace); err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, "OK")
}
