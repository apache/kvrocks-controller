package meta

const (
	RoleMaster = "master"
	RoleSlave  = "slave"
)

type NodeInfo struct {
	ID          string
	CreatedAt   int64
	Address     string
	Role        string
	RequirePass string
	MasterAuth  string
}

func (nodeInfo *NodeInfo) Validate() error {
	// TODO: validate the require fields
	return nil
}

func (nodeInfo *NodeInfo) IsMaster() bool {
	return nodeInfo.Role == RoleMaster
}
