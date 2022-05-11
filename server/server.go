package server

import (
	"context"
	"fmt"
	"net/http"

	"github.com/KvrocksLabs/kvrocks-controller/storage"

	"github.com/KvrocksLabs/kvrocks-controller/storage/base/memory"

	"github.com/KvrocksLabs/kvrocks-controller/controller"

	"github.com/gin-gonic/gin"
)

type Server struct {
	stor       storage.Storage
	controller *controller.Controller
	engine     *gin.Engine
	httpServer *http.Server
}

func NewServer() (*Server, error) {
	stor := memory.NewMemStorage()
	ctrl, err := controller.New(stor)
	if err != nil {
		return nil, err
	}
	return &Server{
		stor:       stor,
		controller: ctrl,
	}, nil
}

func (srv *Server) Start() error {
	if err := srv.controller.Start(); err != nil {
		return err
	}

	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	SetupRoute(srv, engine)
	httpServer := &http.Server{
		Addr:    "127.0.0.1:8080",
		Handler: engine,
	}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil {
			if err == http.ErrServerClosed {
				return
			}
			panic(fmt.Errorf("API server failed: %s", err))
		}
	}()
	srv.engine = engine
	srv.httpServer = httpServer
	return nil
}

func (srv *Server) Stop(ctx context.Context) error {
	_ = srv.controller.Stop()
	return srv.httpServer.Shutdown(ctx)
}
