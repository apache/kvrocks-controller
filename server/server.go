package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/KvrocksLabs/kvrocks_controller/storage/persistence/etcd"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/KvrocksLabs/kvrocks_controller/controller"
	"github.com/KvrocksLabs/kvrocks_controller/controller/failover"
	"github.com/KvrocksLabs/kvrocks_controller/controller/migrate"
	"github.com/KvrocksLabs/kvrocks_controller/controller/probe"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/gin-gonic/gin"
)

type Server struct {
	engine      *gin.Engine
	storage     *storage.Storage
	migration   *migrate.Migrate
	failover    *failover.FailOver
	healthProbe *probe.Probe
	controller  *controller.Controller
	config      *Config
	httpServer  *http.Server
	adminServer *http.Server
}

func NewServer(cfg *Config) (*Server, error) {
	cfg.init()
	persist, err := etcd.New(cfg.Addr, "/kvrocks/controller/leader", cfg.Etcd.Addrs)
	if err != nil {
		return nil, err
	}
	storage, err := storage.NewStorage(persist)
	if err != nil {
		return nil, err
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

func (srv *Server) startAdminServer() {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Any("/debug/pprof/*profile", PProf)
	engine.GET("/metrics", gin.WrapH(promhttp.Handler()))

	httpServer := &http.Server{
		Addr:    srv.config.Admin.Addr,
		Handler: engine,
	}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil {
			if err == http.ErrServerClosed {
				return
			}
			panic(fmt.Errorf("API server: %w", err))
		}
	}()
	srv.adminServer = httpServer
}

func (srv *Server) Start() error {
	if err := srv.controller.Start(); err != nil {
		return err
	}
	srv.startAPIServer()
	srv.startAdminServer()
	return nil
}

func (srv *Server) Stop() error {
	_ = srv.controller.Stop()
	gracefulCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return srv.httpServer.Shutdown(gracefulCtx)
}
