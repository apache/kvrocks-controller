package server

import (
	"errors"

	"github.com/RocksLabs/kvrocks_controller/storage"
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
	responseOK(c, gin.H{"namespaces": namespaces})
}

func (handler *NamespaceHandler) Exists(c *gin.Context) {
	namespace := c.Param("namespace")
	ok, err := handler.storage.IsNamespaceExists(c, namespace)
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, gin.H{"exists": ok})
}

func (handler *NamespaceHandler) Create(c *gin.Context) {
	var request struct {
		Namespace string `json:"namespace"`
	}
	if err := c.BindJSON(&request); err != nil {
		responseBadRequest(c, err)
		return
	}
	if len(request.Namespace) == 0 {
		responseBadRequest(c, errors.New("namespace should NOT be empty"))
		return
	}

	if err := handler.storage.CreateNamespace(c, request.Namespace); err != nil {
		responseError(c, err)
		return
	}
	responseCreated(c, "created")
}

func (handler *NamespaceHandler) Remove(c *gin.Context) {
	namespace := c.Param("namespace")
	if err := handler.storage.RemoveNamespace(c, namespace); err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, "ok")
}
