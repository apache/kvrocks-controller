package failover

const (
	TaskInit = iota
	TaskPending
	TaskDoing
	TaskSuccess
	TaskFailed
)

const (
	AutoType = iota
	ManualType
)

var (
	// PingInterval stands ping period, at least more than double ProbeInterval
	PingInterval = 5

	MaxPingCount = 2

	// MinAliveSize is min number of cluster nodes to enter the safe mode
	MinAliveSize = 10

	// MaxFailureRaito is gate value, more than nodes failed enter the safe mode
	MaxFailureRaito = 0.4

	GCInterval = 1
)
