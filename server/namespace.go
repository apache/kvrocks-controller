/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

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
