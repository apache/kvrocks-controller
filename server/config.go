package server

type EtcdConfig struct {
	Addrs []string `yaml:"addrs"`
}

type AdminConfig struct {
	Addr string `yaml:"addr"`
}

type Config struct {
	Addr  string      `yaml:"addr"`
	Etcd  *EtcdConfig `yaml:"etcd"`
	Admin AdminConfig `yaml:"admin"`
}

func (c *Config) init() {
	if c == nil {
		*c = Config{}
	}
	if c.Addr == "" {
		c.Addr = "127.0.0.1:9379"
	}
	if c.Etcd == nil {
		c.Etcd = &EtcdConfig{
			Addrs: []string{"127.0.0.1:2379"},
		}
	}
	if c.Admin.Addr == "" {
		c.Admin.Addr = "127.0.0.1:2379"
	}
}
