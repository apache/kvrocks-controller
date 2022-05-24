package server

import (
	"context"
	"fmt"
	"net/http"

	"github.com/KvrocksLabs/kvrocks-controller/consts"
	"github.com/KvrocksLabs/kvrocks-controller/storage"
	"github.com/KvrocksLabs/kvrocks-controller/controller"
	"github.com/KvrocksLabs/kvrocks-controller/migrate"
	"github.com/KvrocksLabs/kvrocks-controller/failover"
	"github.com/gin-gonic/gin"
)

type ControllerConfig struct {
	Addr      string   `yaml:"controller"`
	EtcdAddrs []string `yaml:"etcdhosts"`
}

func deafultConfig() *ControllerConfig {
	return &ControllerConfig{
		Addr:      "127.0.0.1:9379",
		EtcdAddrs: []string{"127.0.0.1:2379"},
	}
}

type Server struct {
	stor       *storage.Storage
	migr       *migrate.Migrate
	fovr       *failover.Failover
	probe      *controller.HealthProbe
	controller *controller.Controller
	config     *ControllerConfig
	engine     *gin.Engine
	httpServer *http.Server
}

func NewServer(cfg *ControllerConfig) (*Server, error) {
	if cfg == nil {
		cfg = deafultConfig()
	}
	stor, err := storage.NewStorage(cfg.Addr, cfg.EtcdAddrs)
	if err != nil {
		return nil, err
	}

	migra := migrate.NewMigrate(stor)
	fover := failover.NewFailover(stor)
	probe := controller.NewHealthProbe(stor, fover)
	prces := controller.NewProcesses()
	prces.Register(consts.ContextKeyStorage, stor)
	prces.Register(consts.ContextKeyMigrate, migra)
	prces.Register(consts.ContextKeyFailover,fover)
	prces.Register(consts.ContextKeyHealthy, probe)

	ctrl, err := controller.New(prces)
	if err != nil {
		return nil, err
	}
	return &Server{
		stor:       stor,
		migr:       migra,
		fovr:       fover,
		probe:      probe,
		controller: ctrl,
		config:     cfg,
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
		Addr:    srv.config.Addr,
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
