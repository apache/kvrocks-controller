package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStorage_ClusterInfoCmd(t *testing.T) {
	info, _ := ClusterInfoCmd("127.0.0.1:6379")
	assert.Equal(t, true, info.ClusterState)
	assert.Equal(t, 16384, info.ClusterSlotsAssigned)
	assert.Equal(t, 16384, info.ClusterSlotsOK)
	assert.Equal(t, 0, info.ClusterSlotsPFail)
	assert.Equal(t, 0, info.ClusterSlotsFail)
	assert.Equal(t, 4, info.ClusterKnownNodes)
	assert.Equal(t, uint64(8), info.ClusterCurrentEpoch)
	assert.Equal(t, uint64(8), info.ClusterMyEpoch)
	assert.Equal(t, 7008, info.MigratingSlot)
	assert.Equal(t, -1, info.ImportingSlot)
	assert.Equal(t, "6d536c7eb1df4112e0b51ddb69ec5a5b7959390d", info.DestinationNode)
	assert.Equal(t, "success", info.MigratingState)
	assert.Equal(t, "", info.ImportingState)
}
