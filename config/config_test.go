package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig(t *testing.T) {
	cfg := Config{}

	cfg.Addr = ""
	cfg.Init()
	t.Log(cfg.Addr) // 172.16.40.81:9379

	cfg.Addr = ":8080"
	cfg.Init()
	t.Log(cfg.Addr) // 172.16.40.81:8080

	addr := "1.1.1.1:8080"
	cfg.Addr = addr
	cfg.Init()
	assert.Equal(t, addr, cfg.Addr)

	os.Setenv("KVROCKS_CONTROLLER_HTTP_HOST", "1.2.3.4")
	os.Setenv("KVROCKS_CONTROLLER_HTTP_PORT", "8080")
	cfg.Init()
	assert.Equal(t, "1.2.3.4:8080", cfg.Addr)

	// unset env, avoid environmental pollution
	os.Setenv("KVROCKS_CONTROLLER_HTTP_HOST", "")
	os.Setenv("KVROCKS_CONTROLLER_HTTP_PORT", "")
}

func TestDefaultControllerConfigSet(t *testing.T) {
	cfg := Config{}
	cfg.Init()

	expectedControllerConfig := &ControllerConfig{
		FailOver: &FailOverConfig{
			GCIntervalSeconds:   3600,
			PingIntervalSeconds: 5,
			MaxPingCount:        4,
			MinAliveSize:        10,
			MaxFailureRatio:     0.6,
		},
	}

	assert.Equal(t, expectedControllerConfig, cfg.Controller)
}
