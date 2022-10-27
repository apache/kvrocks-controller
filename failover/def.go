package failover

const (
	TaskQueued = iota + 1
	TaskStarted
	TaskSuccess
	TaskFailed
)

const (
	AutoType = iota + 1
	ManualType
)

var (
	// PingInterval stands ping period, at least more than double ProbeInterval
	PingInterval = 6

	MaxPingCount = 2

	// MinAliveSize is min number of cluster nodes to enter the safe mode
	MinAliveSize = 10

	// MaxFailureRatio is gate value, more than nodes failed enter the safe mode
	MaxFailureRatio = 0.4

	GCInterval = 1
)
