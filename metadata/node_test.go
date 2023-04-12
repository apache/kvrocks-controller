package metadata

import (
	"testing"

	"github.com/KvrocksLabs/kvrocks_controller/util"

	"github.com/stretchr/testify/require"
)

func TestNodeInfo_Validate(t *testing.T) {
	node := &NodeInfo{}
	require.EqualError(t, node.Validate(), "node id shouldn't be empty")
	node.ID = "1234"
	require.EqualError(t, node.Validate(), "the length of node id must be 40")
	node.ID = util.RandString(NodeIdLen)
	require.EqualError(t, node.Validate(), "node role should be 'master' or 'slave'")
	node.Role = RoleMaster
	node.Addr = "1.2.3.4"
	require.NoError(t, node.Validate())
}
