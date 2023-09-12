package config

import (
	"errors"
	"fmt"
	"net"
	"os"

	"github.com/RocksLabs/kvrocks_controller/storage/persistence/etcd"
	"github.com/go-playground/validator/v10"
)

type AdminConfig struct {
	Addr string `yaml:"addr"`
}

type FailOverConfig struct {
	GCIntervalSeconds   int     `yaml:"gc_interval_seconds"`
	PingIntervalSeconds int     `yaml:"ping_interval_seconds"`
	MaxPingCount        int     `yaml:"max_ping_count"`
	MinAliveSize        int     `yaml:"min_alive_size"`
	MaxFailureRatio     float64 `yaml:"max_failure_ratio"`
}

type ControllerConfig struct {
	FailOver *FailOverConfig `yaml:"failover"`
}

const defaultPort = 9379

type Config struct {
	Addr       string            `yaml:"addr"`
	Etcd       *etcd.Config      `yaml:"etcd"`
	Admin      AdminConfig       `yaml:"admin"`
	Controller *ControllerConfig `yaml:"controller"`
}

func (c *Config) Init() {
	if c == nil {
		*c = Config{}
	}

	c.Addr = c.getAddr()
	if c.Etcd == nil {
		c.Etcd = &etcd.Config{
			Addrs: []string{"127.0.0.1:2379"},
		}
	}

	if c.Controller == nil {
		c.Controller = &ControllerConfig{}
	}
	if c.Controller.FailOver == nil {
		c.Controller.FailOver = getDefaultFailOverConfig()
	}
}

func (c *Config) Validate() error {
	if c.Controller.FailOver.MaxPingCount < 3 {
		return errors.New("max ping count required >= 3")
	}
	if c.Controller.FailOver.GCIntervalSeconds < 60 {
		return errors.New("gc interval required >= 1min")
	}
	if c.Controller.FailOver.PingIntervalSeconds < 1 {
		return errors.New("ping interval required >= 1s")
	}
	if c.Controller.FailOver.MinAliveSize < 2 {
		return errors.New("min alive size required >= 2")
	}
	return nil
}

func getDefaultFailOverConfig() *FailOverConfig {
	return &FailOverConfig{
		GCIntervalSeconds:   3600,
		PingIntervalSeconds: 5,
		MaxPingCount:        4,
		MinAliveSize:        10,
		MaxFailureRatio:     0.6,
	}
}

func (c *Config) getAddr() string {
	// env has higher priority than configuration.
	// case: get addr from env
	checker := validator.New()
	host := os.Getenv("KVROCKS_CONTROLLER_HTTP_HOST")
	port := os.Getenv("KVROCKS_CONTROLLER_HTTP_PORT")
	addr := host + ":" + port
	err := checker.Var(addr, "required,tcp_addr")
	if err == nil {
		return fmt.Sprintf("%s:%s", host, port)
	}
	if c.Addr != "" {
		return c.Addr
	}

	// case: addr is empty
	ip := getLocalIP()
	if ip != "" {
		return fmt.Sprintf("%s:%d", ip, defaultPort)
	}
	return fmt.Sprintf("127.0.0.1:%d", defaultPort)
}

// getLocalIP returns the non loopback local IP of the host.
func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		ipnet, ok := address.(*net.IPNet)
		if ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}
