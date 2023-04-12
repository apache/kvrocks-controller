package metadata

import (
	"errors"

	"github.com/go-playground/validator/v10"
)

const (
	RoleMaster = "master"
	RoleSlave  = "slave"
)

var (
	NodeIdLen = 40
)

var _validator = validator.New()

type NodeInfo struct {
	ID         string `json:"id" validate:"required"`
	Addr       string `json:"addr" validate:"required"`
	Role       string `json:"role" validate:"required"`
	Password   string `json:"password"`
	MasterAuth string `json:"master_auth"`
	CreatedAt  int64  `json:"created_at"`
}

func (nodeInfo *NodeInfo) Validate() error {
	if len(nodeInfo.ID) == 0 {
		return errors.New("node id shouldn't be empty")
	}
	if len(nodeInfo.ID) != NodeIdLen {
		return errors.New("the length of node id must be 40")
	}
	if nodeInfo.Role != RoleMaster && nodeInfo.Role != RoleSlave {
		return errors.New("node role should be 'master' or 'slave'")
	}
	// TODO: check the node address format
	return _validator.Struct(nodeInfo)
}

func (nodeInfo *NodeInfo) IsMaster() bool {
	return nodeInfo.Role == RoleMaster
}
