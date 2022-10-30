package migrate

import (
	"errors"
)

var (
	// ErrEmptyMigrateTask is returned if the task list is empty
	ErrEmptyMigrateTask = errors.New("empty migrate task")

	// ErrMismatchTaskNamespace is returned if add tasks has namespace different
	ErrMismatchTaskNamespace = errors.New("add migrate tasks namespace mismatch")

	// ErrMismatchTasksCluster is returned if add tasks has cluster different
	ErrMismatchTasksCluster = errors.New("add migrate tasks cluster mismatch")

	// ErrMismatchTasksID is returned if add tasks has taskid different
	ErrMismatchTasksID = errors.New("add migrate tasks taskid mismatch")

	// ErrTaskHasExisted means duplicate task
	ErrTaskHasExisted = errors.New("migrate task has existed")

	// ErrUnknownTaskType is returned if get tasks without 'pending, doing, history'
	ErrUnknownTaskType = errors.New("unknown migrate task type")

	// ErrMigrateTaskTimeout is returned if on slot migrate timeout
	ErrMigrateTaskTimeout = errors.New("migrate task timeout")

	// ErrMigrateSlotNoExists is returned if slot do noi in source
	ErrMigrateSlotNoExists = errors.New("migrate source slot no exists")

	// ErrMismatchMigrateSlot is returned if migrating slot is different kvrocks-node migrating slot
	ErrMismatchMigrateSlot = errors.New("mismatched migrate slot")

	// ErrMigrateSlotFail from kvrocks-node that migrate fail
	ErrMigrateSlotFail = errors.New("migrate slot fail")

	// ErrMigrateSlotConflict from kvrocks-node, will ignore
	ErrMigrateSlotConflict = errors.New("only one migrating task is allowed at the same time")

	// ErrMigrateSlotCompleted from kvrocks-node, will ignore
	ErrMigrateSlotCompleted = errors.New("migrate slot task has been completed")

	// ErrMigrateNotReady is returned when data is loading or switch slave
	ErrMigrateNotReady = errors.New("migrate not ready, slave or loading")

	// ErrAbortMigrateTask is returned when migrate slot err
	ErrAbortMigrateTask = errors.New("abort migrate task")

	// ErrAbortMigrateSlot is returned when migrate slot has completed
	ErrAbortMigrateSlot = errors.New("abort migrate slot")

	// ErrAbortMigrateRoutine is returned when finish migrate goroutine
	ErrAbortMigrateRoutine = errors.New("abort migrate routine")
)

var (
	// TaskCheckInterval second check kvrocks-node migrate status
	TaskCheckInterval = 1

	// TaskCheckMaxCount * MigrateTaskCheckInterval migrate timeout
	TaskCheckMaxCount = 24 * 60 * 60

	// SlotFail check kvrocks-node migrate status result
	SlotFail = "fail"

	// SlotSuccess check kvrocks-node migrate status result
	SlotSuccess = "success"

	// SlotSleepInterval sleep time(second) slot by slot
	// during sleep controller will sync topo to cluster node
	// TODO: support blocking sync or asynchronous notifications when sync topo
	SlotSleepInterval = 1
)

const (
	TaskInit    = iota // create task init
	TaskPending        // push tasks queue
	TaskDoing          // pop from queue, add doing
	TaskSuccess        // remove from doing, err is nil
	TaskFail           // remove from doing, err not nil
)
