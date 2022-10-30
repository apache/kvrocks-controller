package migrate

import (
	"context"
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

// NewMigrate creates migrate instance, need to fire the storage to schedule tasks
func NewMigrate(storage *storage.Storage) *Migrate {
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
			mig.abortTask(task, err, nil)
			continue
		}
		sourceNode, err := mig.storage.GetMasterNode(namespace, cluster, task.Source)
		if err != nil {
			mig.abortTask(task, err, nil)
			continue
		}
		targetNode, err := mig.storage.GetMasterNode(namespace, cluster, task.Target)
		if err != nil {
			mig.abortTask(task, err, nil)
			continue
		}
		cli, err := util.NewRedisClient(sourceNode.Address)
		if err != nil {
			mig.abortTask(task, err, nil)
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
					mig.abortTask(task, err, cli)
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
			mig.abortTask(task, err, cli)
			return ErrAbortMigrateTask
		}
		if clusterInfo.MigratingSlot == task.SlotDoing && clusterInfo.MigratingState == SlotSuccess {
			if err := mig.storage.MigrateSlot(task.Namespace, task.Cluster, task.Source, task.Target, task.SlotDoing); err != nil {
				mig.abortTask(task, err, cli)
				return ErrAbortMigrateTask
			}
			return ErrAbortMigrateSlot
		}
	}
	task.SlotDoing = slot

	has, err := mig.storage.HasSlot(task.Namespace, task.Cluster, task.Source, slot)
	if err != nil {
		mig.abortTask(task, err, cli)
		return ErrAbortMigrateTask
	}
	if !has {
		mig.abortTask(task, ErrMigrateSlotNoExists, cli)
		return ErrAbortMigrateTask
	}

	err = cli.Do(context.Background(), "CLUSTERX", "migrate", strconv.Itoa(slot), target.ID).Err()
	if err != nil {
		switch err.Error() {
		case ErrMigrateSlotCompleted.Error():
			if err := mig.storage.MigrateSlot(task.Namespace, task.Cluster, task.Source, task.Target, slot); err != nil {
				mig.abortTask(task, err, cli)
				return ErrAbortMigrateTask
			}
			return ErrAbortMigrateSlot
		case ErrMigrateSlotConflict.Error():
		default:
			mig.abortTask(task, err, cli)
			return ErrAbortMigrateTask
		}
	}

	count := 0
	checkResultTicker := time.NewTicker(time.Duration(TaskCheckInterval) * time.Second)
	defer checkResultTicker.Stop()
	for {
		if count == TaskCheckMaxCount/TaskCheckInterval {
			mig.abortTask(task, ErrMigrateTaskTimeout, cli)
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
				mig.abortTask(task, ErrMismatchMigrateSlot, cli)
				return ErrAbortMigrateTask
			}
			switch clusterInfo.MigratingState {
			case SlotFail:
				mig.abortTask(task, ErrMigrateSlotFail, cli)
				return ErrAbortMigrateTask
			case SlotSuccess:
				if err := mig.storage.MigrateSlot(task.Namespace, task.Cluster, task.Source, task.Target, task.SlotDoing); err != nil {
					mig.abortTask(task, err, cli)
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
