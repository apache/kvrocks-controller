package server

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig(t *testing.T) {
	cfg := Config{}

	cfg.Addr = ""
	cfg.init()
	t.Log(cfg.Addr) // 172.16.40.81:9379

	cfg.Addr = ":8080"
	cfg.init()
	t.Log(cfg.Addr) // 172.16.40.81:8080

	addr := "1.1.1.1:8080"
	cfg.Addr = addr
	cfg.init()
	assert.Equal(t, addr, cfg.Addr)

	os.Setenv("KVROCKS_CONTROLLER_HTTP_HOST", "1.2.3.4")
	os.Setenv("KVROCKS_CONTROLLER_HTTP_PORT", "8080")
	cfg.init()
	assert.Equal(t, "1.2.3.4:8080", cfg.Addr)

	// unset env, avoid environmental pollution
	os.Setenv("KVROCKS_CONTROLLER_HTTP_HOST", "")
	os.Setenv("KVROCKS_CONTROLLER_HTTP_PORT", "")
}
