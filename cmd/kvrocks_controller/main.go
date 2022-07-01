package main

import (
	"context"
	"flag"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"

	"github.com/KvrocksLabs/kvrocks_controller/logger"
	"github.com/KvrocksLabs/kvrocks_controller/server"
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
	// parser parameter
	flag.Parse()

	// load config
	var serCfg *server.ControllerConfig
	if len(configPath) != 0 {
		serCfg = &server.ControllerConfig{}
		content, err := ioutil.ReadFile(configPath)
		if err != nil {
			logger.Get().With(zap.Error(err)).Error("read config file failed!")
			return
		}
		err = yaml.Unmarshal(content, serCfg)
		if err != nil {
			logger.Get().With(zap.Error(err)).Error("unmarshal config file failed!")
			return
		}
	}

	// os signal handler
	shutdownCh := make(chan struct{})
	registerSignal(shutdownCh)

	// start server
	srv, err := server.NewServer(serCfg)
	if err != nil {
		logger.Get().With(zap.Error(err)).Error("Failed to create the server")
		return
	}
	if err := srv.Start(); err != nil {
		logger.Get().With(zap.Error(err)).Error("Failed to start the server")
		return
	}

	// wait for the term signal
	<-shutdownCh
	if err := srv.Stop(context.Background()); err != nil {
		logger.Get().With(zap.Error(err)).Error("Failed to close the server")
	} else {
		logger.Get().Info("Bye bye, Kvrocks controller server exited")
	}
}
