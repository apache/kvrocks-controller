package config

import (
	"github.com/RocksLabs/kvrocks_controller/storage/persistence/etcd"
	"os"
	"fmt"
	"net"
	"github.com/go-playground/validator/v10"
)

type AdminConfig struct {
	Addr string `yaml:"addr"`
}

type FailOverConfig struct {
	GCInterval      int     `yaml:"GCInterval"`
	PingInterval    int     `yaml:"pingInterval"`
	MaxPingCount    int     `yaml:"maxPingCount"`
	MinAliveSize    int     `yaml:"minAliveSize"`
	MaxFailureRatio float64 `yaml:"maxFailureRatio"`
}

type ControllerConfig struct {
	FailOver *FailOverConfig `yaml:"failOver"`
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
		c.Controller = &ControllerConfig{
			FailOver: getDefaultFailOverConfig(),
		}
	}
}

func getDefaultFailOverConfig() *FailOverConfig {
	return &FailOverConfig{
		GCInterval:      1,
		PingInterval:    6,
		MaxPingCount:    2,
		MinAliveSize:    10,
		MaxFailureRatio: 0.4,
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
