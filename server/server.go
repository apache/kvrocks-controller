package server

import (
	"context"
	"fmt"
	"net/http"

	"github.com/KvrocksLabs/kvrocks_controller/consts"
	"github.com/KvrocksLabs/kvrocks_controller/controller"
	"github.com/KvrocksLabs/kvrocks_controller/failover"
	"github.com/KvrocksLabs/kvrocks_controller/migrate"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/gin-gonic/gin"
)

type Config struct {
	Addr        string   `yaml:"addr"`
	EtcdAddrs   []string `yaml:"etcd_addrs"`
	MetricsAddr string   `yaml:"metrics_addr"`
}

func defaultConfig() *Config {
	return &Config{
		Addr:      "127.0.0.1:9379",
		EtcdAddrs: []string{"127.0.0.1:2379"},
	}
}

type Server struct {
	storage     *storage.Storage
	migration   *migrate.Migrate
	failover    *failover.FailOver
	healthProbe *controller.HealthProbe
	controller  *controller.Controller
	config      *Config
	engine      *gin.Engine
	httpServer  *http.Server
}

func NewServer(cfg *Config) (*Server, error) {
	if cfg == nil {
		cfg = defaultConfig()
	}
	storage, err := storage.NewStorage(cfg.Addr, cfg.EtcdAddrs)
	if err != nil {
		return nil, err
	}

	migration := migrate.NewMigrate(storage)
	failover := failover.NewFailOver(storage)
	healthProbe := controller.NewHealthProbe(storage, failover)
	batchProcessor := controller.NewBatchProcessor()
	_ = batchProcessor.Register(consts.ContextKeyStorage, storage)
	_ = batchProcessor.Register(consts.ContextKeyMigrate, migration)
	_ = batchProcessor.Register(consts.ContextKeyFailover, failover)
	_ = batchProcessor.Register(consts.ContextKeyHealthy, healthProbe)

	ctrl, err := controller.New(batchProcessor)
	if err != nil {
		return nil, err
	}
	return &Server{
		storage:     storage,
		migration:   migration,
		failover:    failover,
		healthProbe: healthProbe,
		controller:  ctrl,
		config:      cfg,
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
			panic(fmt.Errorf("API server: %w", err))
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
