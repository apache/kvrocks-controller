package failover

const (
	TaskInit = iota // create task init
	TaskPending     // push tasks queue
	TaskDoing       // pop from queue, add doing
	TaskSuccess     // remove from doing, err is nil
	TaskFail        // remove from doing, err not nil
)

const (
	AutoType = iota // from healthy probe
	ManualType      // from http request, for devops
)

/* 
 * the task that add FailoverNode is pfail status
 * after FailoverCount times ping fail, enter fail
 */
var (
	// FailoverInterval stands ping period, at least more than double ProbeInterval
	FailoverInterval = 4

	// FailoverCount times ping failed, handle fail nodes
	FailoverCount    = 2

	// FailoverMinSize is min number of cluster nodes to enter safemode
	FailoverMinSize  = 10

	// FailoverRaito is gate value, more than nodes failed enter safemode
	FailoverRaito    = 0.4

	// GcFailoverInterval collection Failover.space memory
	GcFailoverInterval = 1
)