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
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/RocksLabs/kvrocks_controller/storage/persistence/etcd"

	"github.com/RocksLabs/kvrocks_controller/controller"
	"github.com/RocksLabs/kvrocks_controller/controller/failover"
	"github.com/RocksLabs/kvrocks_controller/controller/probe"
	"github.com/RocksLabs/kvrocks_controller/storage"
	"github.com/gin-gonic/gin"
)

type Server struct {
	engine      *gin.Engine
	storage     *storage.Storage
	failover    *failover.Failover
	healthProbe *probe.Probe
	controller  *controller.Controller
	config      *Config
	httpServer  *http.Server
}

func NewServer(cfg *Config) (*Server, error) {
	cfg.init()
	persist, err := etcd.New(cfg.Addr, "/kvrocks/controller/leader", cfg.Etcd)
	if err != nil {
		return nil, err
	}
	storage, err := storage.NewStorage(persist)
	if err != nil {
		return nil, err
	}
	if ok := storage.IsReady(); !ok {
		return nil, fmt.Errorf("storage is not ready")
	}

	ctrl, err := controller.New(storage)
	if err != nil {
		return nil, err
	}
	gin.SetMode(gin.ReleaseMode)
	return &Server{
		storage:    storage,
		controller: ctrl,
		config:     cfg,
		engine:     gin.New(),
	}, nil
}

func (srv *Server) startAPIServer() {
	srv.initHandlers()
	httpServer := &http.Server{
		Addr:    srv.config.Addr,
		Handler: srv.engine,
	}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil {
			if err == http.ErrServerClosed {
				return
			}
			panic(fmt.Errorf("API server: %w", err))
		}
	}()
	srv.httpServer = httpServer
}

func PProf(c *gin.Context) {
	switch c.Param("profile") {
	case "/cmdline":
		pprof.Cmdline(c.Writer, c.Request)
	case "/symbol":
		pprof.Symbol(c.Writer, c.Request)
	case "/profile":
		pprof.Profile(c.Writer, c.Request)
	case "/trace":
		pprof.Trace(c.Writer, c.Request)
	default:
		pprof.Index(c.Writer, c.Request)
	}
}

func (srv *Server) Start() error {
	if err := srv.controller.Start(); err != nil {
		return err
	}
	srv.startAPIServer()
	return nil
}

func (srv *Server) Stop() error {
	_ = srv.controller.Stop()
	gracefulCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return srv.httpServer.Shutdown(gracefulCtx)
}
