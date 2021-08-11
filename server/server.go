package server

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
)

type Server struct {
	engine     *gin.Engine
	httpServer *http.Server
}

func NewServer() (*Server, error) {
	return &Server{}, nil
}

func (srv *Server) Start() {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	SetupRoute(engine)

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
}

func (srv *Server) Stop(ctx context.Context) error {
	return srv.httpServer.Shutdown(ctx)
}
