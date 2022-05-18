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
	NodeIdMinLen = 8
)

var _validator = validator.New()

type NodeInfo struct {
	ID              string `json:"id" validate:"required"`
	CreatedAt       int64  `json:"created_at"`
	Address         string `json:"address" validate:"required"`
	Role            string `json:"role" validate:"required"`
	RequirePassword string `json:"require_password"`
	MasterAuth      string `json:"master_auth"`
}

func (nodeInfo *NodeInfo) Validate() error {
	// TODO: id should be fixed length
	if len(nodeInfo.ID) == 0 {
		return errors.New("node id shouldn't be empty")
	}
	if nodeInfo.Role != RoleMaster && nodeInfo.Role != RoleSlave {
		return errors.New("node role should be 'master' or 'slave'")
	}
	return _validator.Struct(nodeInfo)
}

func (nodeInfo *NodeInfo) IsMaster() bool {
	return nodeInfo.Role == RoleMaster
}
