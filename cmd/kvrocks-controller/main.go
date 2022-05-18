package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/KvrocksLabs/kvrocks-controller/server"
)

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
	shutdownCh := make(chan struct{})
	registerSignal(shutdownCh)
	srv, _ := server.NewServer(nil)
	srv.Start()
	// wait for the term signal
	<-shutdownCh
	if err := srv.Stop(context.Background()); err != nil {
		// TODO: log error here
	}
}
