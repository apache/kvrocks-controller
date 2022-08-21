package main

import (
	"context"
	"flag"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/KvrocksLabs/kvrocks_controller/logger"
	"github.com/KvrocksLabs/kvrocks_controller/metrics"
	"github.com/KvrocksLabs/kvrocks_controller/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"gopkg.in/yaml.v1"
)

var (
	configPath string
)

func init() {
	flag.StringVar(&configPath, "c", "", "set config yaml file path")
}

func registerSignal(shutdown chan struct{}) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, []os.Signal{syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1}...)
	go func() {
		for sig := range c {
			if handleSignals(sig) {
				close(shutdown)
				return
			}
		}
	}()
}

func handleSignals(sig os.Signal) (exitNow bool) {
	switch sig {
	case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM:
		return true
	case syscall.SIGUSR1:
		return false
	}
	return false
}

func main() {
	// os signal handler
	shutdownCh := make(chan struct{})
	registerSignal(shutdownCh)

	flag.Parse()

	config := &server.Config{}
	if len(configPath) != 0 {
		content, err := ioutil.ReadFile(configPath)
		if err != nil {
			logger.Get().With(zap.Error(err)).Error("Failed to read the config file")
			return
		}
		if err := yaml.Unmarshal(content, config); err != nil {
			logger.Get().With(zap.Error(err)).Error("Failed to unmarshal the config file")
			return
		}
	}

	srv, err := server.NewServer(config)
	if err != nil {
		logger.Get().With(zap.Error(err)).Error("Failed to create the server")
		return
	}
	if err := srv.Start(); err != nil {
		logger.Get().With(zap.Error(err)).Error("Failed to start the server")
		return
	}
	if len(config.MetricsAddr) != 0 {
		metrics.MustRegisterMetrics()
		go func(metricsAddr string) {
			http.Handle("/metrics", promhttp.Handler())
			_ = http.ListenAndServe(metricsAddr, nil)
		}(config.MetricsAddr)
	}

	// wait for the term signal
	<-shutdownCh
	if err := srv.Stop(context.Background()); err != nil {
		logger.Get().With(zap.Error(err)).Error("Failed to close the server")
	} else {
		logger.Get().Info("Bye bye, Kvrocks controller was exited")
	}
}
