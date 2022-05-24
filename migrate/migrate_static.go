package migrate

import (
	"errors"
)

var (
	// ErrAddMigTasksEmpty is returned if add tasks list is empty
	ErrAddMigTasksEmpty = errors.New("add migrate tasks can not empty")

	// ErrAddMigTasksNsMistmatch is returned if add tasks has namespace different
	ErrAddMigTasksNsMistmatch = errors.New("add migrate tasks namespace mismatch")

	// ErrAddMigTasksClusterMistmatch is returned if add tasks has cluster different
	ErrAddMigTasksClusterMistmatch = errors.New("add migrate tasks cluster mismatch")

	// ErrAddMigTasksIDMistmatch is returned if add tasks has taskid different
	ErrAddMigTasksIDMistmatch = errors.New("add migrate tasks taskid mismatch")

	// ErrAddMigTasksHasExisted is returned if add tasks has existed by ns/cluster/taskid same
	ErrAddMigTasksHasExisted = errors.New("add migrate tasks has existed")

	// ErrGetMigTasksTypeMistmatch is returned if get tasks without 'pending, doing, history'
	ErrGetMigTasksTypeMistmatch = errors.New("get migrate tasks type error")

	// ErrMigrateTaskTimeout is returned if on slot migrate timeout
	ErrMigrateTaskTimeout = errors.New("migrate task timeout")

	// ErrMigrateSlotNoExists is returned if slot do noi in source
	ErrMigrateSlotNoExists = errors.New("source no migrate slot")

	// ErrMigrateSlotMismatch is returned if migrating slot is different kvrocks-node migrating slot
	ErrMigrateSlotMismatch = errors.New("migrate slot mismatch")
	
	// ErrMigrateSlotFail from kvrocks-node that migrate fail
	ErrMigrateSlotFail = errors.New("node migrate slot fail")

	// ErrMigrateSlotDoing from kvrocks-node, will ignore
	ErrMigrateSlotDoing =errors.New("There is already a migrating slot")
	
	// ErrMigrateSlotCompleted from kvrocks-node, will ignore
	ErrMigrateSlotCompleted = errors.New("Can't migrate slot which has been migrated")

	// ErrMigrateNotReady is returned when data is loading or switch slave
	ErrMigrateNotReady = errors.New("migrate not ready, slave or loading")
	
	// ErrAbortTask is returned when migrate slot err
	ErrAbortTask = errors.New("abort task")

	// ErrAbortSlot is returned when migrate slot has completed
	ErrAbortSlot = errors.New("abort slot")

	// ErrAbortMigrate is returned when finish migrate goroutine
	ErrAbortMigrate = errors.New("abort migrate")
)

var (
	// MigrateTaskCheckInterval second check kvrocks-node migrate status
	MigrateTaskCheckInterval = 1 

	// MigrateTaskCheckMaxCount * MigrateTaskCheckInterval migrate timeout 
	MigrateTaskCheckMaxCount = 24 * 60 * 60

	// MigrateSlotFail check kvrocks-node migrate status result
	MigrateSlotFail = "fail"

	// MigrateSlotFail check kvrocks-node migrate status result
	MigrateSlotSuccess = "success"
)

const (
	TaskInit = iota // create task init
	TaskPending     // push tasks queue
	TaskDoing       // pop from queue, add doing
	TaskSuccess     // remove from doing, err is nil
	TaskFail        // remove from doing, err not nil
)