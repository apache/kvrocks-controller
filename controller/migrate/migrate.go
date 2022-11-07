package migrate

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/KvrocksLabs/kvrocks_controller/logger"
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/KvrocksLabs/kvrocks_controller/storage/persistence/etcd"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
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

// Migrate implement tasks queue and doing task in memory
// schedule tasks and interact migrate storage(etcd)
type Migrate struct {
	storage *storage.Storage
	ready   bool
	tasks   map[string][]*etcd.MigrateTask // memory tasks queue, group by `namespace/cluster`
	doing   map[string]*etcd.MigrateTask   // doing task, group by `namespace/cluster`

	notifyCh  chan *etcd.MigrateTask // notify when push tasks queue
	stopCh    chan struct{}
	quitCh    chan struct{}
	closeOnce sync.Once
	rw        sync.RWMutex
}

// New creates migrate instance, need to fire the storage to schedule tasks
func New(storage *storage.Storage) *Migrate {
	migrate := &Migrate{
		storage:  storage,
		tasks:    make(map[string][]*etcd.MigrateTask),
		doing:    make(map[string]*etcd.MigrateTask),
		notifyCh: make(chan *etcd.MigrateTask, 10),
		stopCh:   make(chan struct{}),
		quitCh:   make(chan struct{}),
	}
	return migrate
}

// Close call by quit or leader-follower switch
func (mig *Migrate) Close() error {
	mig.rw.Lock()
	defer mig.rw.Unlock()
	mig.closeOnce.Do(func() {
		close(mig.quitCh)
		close(mig.notifyCh)
	})
	return nil
}

// Stop migrate instance, will also cancel all goroutines.
func (mig *Migrate) Stop() error {
	mig.rw.Lock()
	defer mig.rw.Unlock()
	if !mig.ready {
		return nil
	}
	mig.ready = false
	close(mig.stopCh)
	return nil
}

func (mig *Migrate) loadDoingTasks() ([]*etcd.MigrateTask, error) {
	var doingTasks []*etcd.MigrateTask

	namespaces, err := mig.storage.ListNamespace()
	if err != nil {
		return nil, err
	}
	for _, namespace := range namespaces {
		clusters, err := mig.storage.ListCluster(namespace)
		if err != nil {
			return nil, err
		}
		for _, cluster := range clusters {
			taskKey := util.BuildClusterKey(namespace, cluster)
			tasks, err := mig.storage.GetMigrateTasks(namespace, cluster)
			if err != nil {
				return nil, err
			}
			if len(tasks) > 0 {
				mig.tasks[taskKey] = tasks
			}
			doing, err := mig.storage.GetDoingMigrateTask(namespace, cluster)
			if err != nil {
				return nil, err
			}
			has, err := mig.storage.IsHistoryMigrateTaskExists(doing)
			if err != nil {
				return nil, err
			}
			if !has && doing != nil {
				doingTasks = append(doingTasks, doing)
				tasks = mig.tasks[taskKey]
				mig.tasks[taskKey] = append([]*etcd.MigrateTask{doing}, tasks...)
			} else if len(tasks) > 0 {
				doingTasks = append(doingTasks, tasks[0])
			}
		}
	}
	return doingTasks, nil
}

// LoadTasks from migrate storage and schedule those tasks
func (mig *Migrate) LoadTasks() error {
	mig.rw.Lock()
	defer mig.rw.Unlock()
	if !mig.storage.IsLeader() {
		return storage.ErrNoLeaderOrNotReady
	}
	doingTasks, err := mig.loadDoingTasks()
	if err != nil {
		return err
	}

	go mig.loop()

	var wg sync.WaitGroup
	go func() {
		wg.Add(1)
		for _, doing := range doingTasks {
			mig.notifyCh <- doing
		}
		wg.Done()
	}()
	wg.Wait()

	mig.stopCh = make(chan struct{})
	mig.ready = true
	return nil
}

// AddTasks push tasks to queue
func (mig *Migrate) AddTasks(tasks []*etcd.MigrateTask) error {
	if !mig.Ready() {
		return ErrMigrateNotReady
	}
	if len(tasks) == 0 {
		return ErrEmptyMigrateTask
	}
	namespace := tasks[0].Namespace
	cluster := tasks[0].Cluster
	taskID := tasks[0].TaskID
	for _, task := range tasks {
		if namespace != task.Namespace {
			return ErrMismatchTaskNamespace
		}
		if cluster != task.Cluster {
			return ErrMismatchTasksCluster
		}
		if taskID != task.TaskID {
			return ErrMismatchTasksID
		}
		// MigrateSlot ascending sort and no overlap
		sort.Slice(task.MigrateSlot, func(i, j int) bool {
			return task.MigrateSlot[i].Start < task.MigrateSlot[j].Start
		})
		task.SlotDoing = -1
		task.Status = TaskPending
		task.PendingTime = time.Now().Unix()
	}
	has, err := mig.storage.IsMigrateTaskExists(tasks[0].Namespace, tasks[0].Cluster, tasks[0].TaskID)
	if err != nil {
		return err
	}
	if has {
		return ErrTaskHasExisted
	}
	if err := mig.addTasks(namespace, cluster, tasks); err != nil {
		return err
	}
	mig.notifyCh <- tasks[0]
	return nil
}

// GetMigrateTasks return tasks by type, support `pending, doing, done`
func (mig *Migrate) GetMigrateTasks(namespace, cluster string, queryType string) ([]*etcd.MigrateTask, error) {
	if !mig.Ready() {
		return nil, ErrMigrateNotReady
	}
	if !mig.storage.IsLeader() {
		return nil, storage.ErrNoLeaderOrNotReady
	}
	name := util.BuildClusterKey(namespace, cluster)
	switch queryType {
	case "pending":
		mig.rw.RLock()
		defer mig.rw.RUnlock()
		if !mig.hasTasks(namespace, cluster) {
			return []*etcd.MigrateTask{}, nil
		}
		return mig.tasks[name], nil
	case "doing":
		mig.rw.RLock()
		defer mig.rw.RUnlock()
		if !mig.hasDoing(namespace, cluster) {
			return []*etcd.MigrateTask{}, nil
		}
		return []*etcd.MigrateTask{mig.doing[name]}, nil
	case "history":
		return mig.storage.GetHistoryMigrateTask(namespace, cluster)
	}
	return nil, ErrUnknownTaskType
}

// Ready return an indicator whether the migrate can work
func (mig *Migrate) Ready() bool {
	mig.rw.RLock()
	defer mig.rw.RUnlock()
	return mig.ready
}

// loop wait tasks come, groupby `namespace/cluster`
func (mig *Migrate) loop() {
	for {
		if !mig.storage.IsLeader() {
			time.Sleep(time.Duration(etcd.SessionTTL) * time.Second)
			continue
		}
		select {
		case task := <-mig.notifyCh:
			if mig.hasDoing(task.Namespace, task.Cluster) {
				continue
			}
			go mig.migrateDoing(task.Namespace, task.Cluster)
		case <-mig.stopCh:
			return
		case <-mig.quitCh:
			return
		}
	}
}

// migrateDoing do tasks by slot
func (mig *Migrate) migrateDoing(namespace, cluster string) {
	for {
	loop:
		select {
		case <-mig.quitCh:
			return
		case <-mig.stopCh:
			return
		default:
		}
		task := mig.removeTask(namespace, cluster)
		if task == nil {
			time.Sleep(time.Duration(SlotSleepInterval) * time.Minute)
			return
		}
		if err := mig.addDoingTask(task); err != nil {
			mig.abortTask(task, err)
			continue
		}
		sourceNode, err := mig.storage.GetMasterNode(namespace, cluster, task.Source)
		if err != nil {
			mig.abortTask(task, err)
			continue
		}
		targetNode, err := mig.storage.GetMasterNode(namespace, cluster, task.Target)
		if err != nil {
			mig.abortTask(task, err)
			continue
		}
		cli, err := util.NewRedisClient(sourceNode.Address)
		if err != nil {
			mig.abortTask(task, err)
			continue
		}
		firstMigrate := true
		for _, slotRange := range task.MigrateSlot {
			for slot := slotRange.Start; slot <= slotRange.Stop; slot++ {
				if task.SlotDoing > slot {
					continue
				}
				time.Sleep(time.Duration(SlotSleepInterval) * time.Second)
				_ = mig.storage.AddDoingMigrateTask(task)
				err := mig.migrateDoingSlot(cli, task, &sourceNode, &targetNode, slot, firstMigrate)
				firstMigrate = false
				if err == nil {
					continue
				}
				switch err.Error() {
				case ErrAbortMigrateSlot.Error():
				case ErrAbortMigrateTask.Error():
					goto loop
				case ErrAbortMigrateRoutine.Error():
					return
				default:
					mig.abortTask(task, err)
					goto loop
				}
			}
		}
		mig.finishTask(task)
	}
}

// migrateDoingSlot doing migrate one slot
func (mig *Migrate) migrateDoingSlot(cli *redis.Client, task *etcd.MigrateTask, source, target *metadata.NodeInfo, slot int, check bool) error {
	/*
	 * scenes: migrate data maybe success, but slot not updata in time
	 * leader-follower switch, new leader check last slot migrate status,
	 * if data migrated, new leader migrate slot and update topo metadata
	 *
	 * TODO: kvrocks_nodes(source and target) update slot and add version
	 * local after success migrated data, or marked the slots when data is
	 * migrated but the slot is not set current node, kvrocks_node should
	 * have the ability to perceive topo changes and queryable.
	 */
	if check {
		clusterInfo, err := util.ClusterInfoCmd(source.Address)
		if err != nil {
			mig.abortTask(task, err)
			return ErrAbortMigrateTask
		}
		if clusterInfo.MigratingSlot == task.SlotDoing && clusterInfo.MigratingState == SlotSuccess {
			if err := mig.storage.MigrateSlot(task.Namespace, task.Cluster, task.Source, task.Target, task.SlotDoing); err != nil {
				mig.abortTask(task, err)
				return ErrAbortMigrateTask
			}
			return ErrAbortMigrateSlot
		}
	}
	task.SlotDoing = slot

	has, err := mig.storage.HasSlot(task.Namespace, task.Cluster, task.Source, slot)
	if err != nil {
		mig.abortTask(task, err)
		return ErrAbortMigrateTask
	}
	if !has {
		mig.abortTask(task, ErrMigrateSlotNoExists)
		return ErrAbortMigrateTask
	}

	err = cli.Do(context.Background(), "CLUSTERX", "migrate", strconv.Itoa(slot), target.ID).Err()
	if err != nil {
		switch err.Error() {
		case ErrMigrateSlotCompleted.Error():
			if err := mig.storage.MigrateSlot(task.Namespace, task.Cluster, task.Source, task.Target, slot); err != nil {
				mig.abortTask(task, err)
				return ErrAbortMigrateTask
			}
			return ErrAbortMigrateSlot
		case ErrMigrateSlotConflict.Error():
		default:
			mig.abortTask(task, err)
			return ErrAbortMigrateTask
		}
	}

	count := 0
	checkResultTicker := time.NewTicker(time.Duration(TaskCheckInterval) * time.Second)
	defer checkResultTicker.Stop()
	for {
		if count == TaskCheckMaxCount/TaskCheckInterval {
			mig.abortTask(task, ErrMigrateTaskTimeout)
			return ErrAbortMigrateTask
		}
		select {
		case <-checkResultTicker.C:
			count++
			clusterInfo, err := util.ClusterInfoCmd(source.Address)
			if err != nil {
				logger.Get().With(
					zap.Error(err),
				).Error("ckeck migrate process, cluster info command")
				continue
			}
			if clusterInfo.MigratingSlot != task.SlotDoing {
				mig.abortTask(task, ErrMismatchMigrateSlot)
				return ErrAbortMigrateTask
			}
			switch clusterInfo.MigratingState {
			case SlotFail:
				mig.abortTask(task, ErrMigrateSlotFail)
				return ErrAbortMigrateTask
			case SlotSuccess:
				if err := mig.storage.MigrateSlot(task.Namespace, task.Cluster, task.Source, task.Target, task.SlotDoing); err != nil {
					mig.abortTask(task, err)
					return ErrAbortMigrateTask
				}
				return nil
			}
		case <-mig.stopCh:
			return ErrAbortMigrateRoutine
		case <-mig.quitCh:
			return ErrAbortMigrateRoutine
		}
	}
}

// hasTasks return an indicator whether `namespace/cluster` has tasks
func (mig *Migrate) hasTasks(namespace, cluster string) bool {
	mig.rw.RLock()
	defer mig.rw.RUnlock()
	_, ok := mig.tasks[util.BuildClusterKey(namespace, cluster)]
	return ok
}

// addTasks will add tasks to queue
func (mig *Migrate) addTasks(namespace, cluster string, tasks []*etcd.MigrateTask) error {
	mig.rw.Lock()
	defer mig.rw.Unlock()
	if err := mig.storage.AddMigrateTask(namespace, cluster, tasks); err != nil {
		return err
	}
	name := util.BuildClusterKey(namespace, cluster)
	mig.tasks[name] = append(mig.tasks[name], tasks...)
	return nil
}

// removeTask remove task from queue, include memory and storage
func (mig *Migrate) removeTask(namespace, cluster string) *etcd.MigrateTask {
	mig.rw.Lock()
	defer mig.rw.Unlock()
	name := util.BuildClusterKey(namespace, cluster)
	tasks, ok := mig.tasks[name]
	if !ok {
		return nil
	}
	task := tasks[0]
	if err := mig.storage.RemoveMigrateTask(task); err != nil {
		logger.Get().With(zap.Error(err)).Error("Failed to remove migrate task from storage")
	}
	if len(tasks) == 1 {
		delete(mig.tasks, name)
	} else {
		mig.tasks[name] = tasks[1:]
	}
	return task
}

// hasDoing return an indicator whether `namespace/cluster` has doing task
func (mig *Migrate) hasDoing(namespace, cluster string) bool {
	mig.rw.RLock()
	defer mig.rw.RUnlock()
	_, ok := mig.doing[util.BuildClusterKey(namespace, cluster)]
	return ok
}

// addDoingTask schedule task to doing, update memory and storage
func (mig *Migrate) addDoingTask(task *etcd.MigrateTask) error {
	mig.rw.Lock()
	defer mig.rw.Unlock()
	task.Status = TaskDoing
	task.DoingTime = time.Now().Unix()
	if err := mig.storage.AddDoingMigrateTask(task); err != nil {
		logger.Get().With(zap.Error(err)).Error("Failed to add the doing task to storage")
		return err
	}
	mig.doing[util.BuildClusterKey(task.Namespace, task.Cluster)] = task
	return nil
}

// removeDoingTaskFromMemory only delete doing task in memory
func (mig *Migrate) removeDoingTaskFromMemory(task *etcd.MigrateTask) {
	mig.rw.Lock()
	defer mig.rw.Unlock()
	task.DoneTime = time.Now().Unix()
	delete(mig.doing, util.BuildClusterKey(task.Namespace, task.Cluster))
}

// abortTask handler task status and push etcd when task exception
func (mig *Migrate) abortTask(task *etcd.MigrateTask, err error) {
	task.Status = TaskFail
	task.Err = err.Error()
	task.DoneTime = time.Now().Unix()
	_ = mig.storage.AddHistoryMigrateTask(task)
	mig.removeDoingTaskFromMemory(task)
	logger.Get().With(
		zap.Error(err),
		zap.Any("task", task),
	).Error("Abort migrate task")
}

// finishTask handler task status and push etcd when task success
func (mig *Migrate) finishTask(task *etcd.MigrateTask) {
	task.Status = TaskSuccess
	_ = mig.storage.AddHistoryMigrateTask(task)
	mig.removeDoingTaskFromMemory(task)
	logger.Get().With(
		zap.Any("task", task),
	).Info("Success to migrate")
}
