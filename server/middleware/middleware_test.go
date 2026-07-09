package middleware

import (
	"testing"

	"github.com/apache/kvrocks-controller/server/helper"
	"github.com/stretchr/testify/assert"
)

func TestShouldRedirect(t *testing.T) {
	tests := []struct {
		name            string
		isLeader        bool
		isRaftMode      bool
		leaderSessionID string
		requestHost     string
		expectRedirect  bool
		expectAddr      string
		expectError     bool
	}{
		{
			name:           "Is leader",
			isLeader:       true,
			isRaftMode:     false,
			expectRedirect: false,
		},
		{
			name:           "Is raft mode",
			isLeader:       false,
			isRaftMode:     true,
			expectRedirect: false,
		},
		{
			name:            "Not leader, not raft, valid peer",
			isLeader:        false,
			isRaftMode:      false,
			leaderSessionID: "127.0.0.1:9379",
			requestHost:     "127.0.0.1:1234",
			expectRedirect:  true,
			expectAddr:      "127.0.0.1:9379",
			expectError:     false,
		},
		{
			name:            "Not leader, not raft, self redirect detected",
			isLeader:        false,
			isRaftMode:      false,
			leaderSessionID: "127.0.0.1:9379",
			requestHost:     "127.0.0.1:9379",
			expectRedirect:  false,
			expectError:     true,
		},
		{
			name:            "Not leader, not raft, valid peer with random session",
			isLeader:        false,
			isRaftMode:      false,
			leaderSessionID: helper.GenerateSessionID("127.0.0.1:9379"),
			requestHost:     "127.0.0.1:1234",
			expectRedirect:  true,
			expectAddr:      "127.0.0.1:9379",
			expectError:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			redirect, addr, err := shouldRedirect(tt.isLeader, tt.isRaftMode, tt.leaderSessionID, tt.requestHost)
			assert.Equal(t, tt.expectRedirect, redirect)
			if tt.expectRedirect {
				assert.Equal(t, tt.expectAddr, addr)
				assert.NoError(t, err)
			} else if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
