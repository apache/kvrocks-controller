package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultControllerConfigSet(t *testing.T) {
	cfg := Default()
	expectedControllerConfig := &ControllerConfig{
		FailOver: &FailOverConfig{
			GCIntervalSeconds:   3600,
			PingIntervalSeconds: 3,
			MaxPingCount:        5,
			MinAliveSize:        10,
			MaxFailureRatio:     0.6,
		},
	}

	assert.Equal(t, expectedControllerConfig, cfg.Controller)
}
