package migrate

import (
	"time"
	"sync"
	"strconv"
	"context"
	"sort"

	"go.uber.org/zap"
	"github.com/go-redis/redis/v8"
	"github.com/KvrocksLabs/kvrocks-controller/logger"
	"github.com/KvrocksLabs/kvrocks-controller/storage"
	"github.com/KvrocksLabs/kvrocks-controller/metadata"
	"github.com/KvrocksLabs/kvrocks-controller/util"
	"github.com/KvrocksLabs/kvrocks-controller/storage/base/etcd"
)

// Migrate implement tasks queue and doing task in memory
// schedule tasks and interact migrate storage(etcd)
type Migrate struct {
	stor  *storage.Storage
	ready bool
	tasks map[string][]*etcd.MigrateTask // memory tasks queue, group by `namespace/cluster`
	doing map[string]*etcd.MigrateTask   // doing task, group by `namespace/cluster`

	notifyCh  chan *etcd.MigrateTask     // notify when push tasks queue 
	stopCh    chan struct{}
	quitCh    chan struct{}
	closeOnce sync.Once
	rw        sync.RWMutex
}

// NewMigrate creates a Migrate, need call Load to schedule tasks
func NewMigrate(stor  *storage.Storage) *Migrate {
	migrate := &Migrate{
		stor:     stor,
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

// Stop all goroutine
func (mig *Migrate) Stop() {
	mig.rw.Lock()
	defer mig.rw.Unlock()
	if !mig.ready {
		return 
	}
	mig.ready = false
	close(mig.stopCh)
}

// Load tasks from migrate storage and begin schedule tasks
func (mig *Migrate) LoadData() error {
	mig.rw.Lock()
	defer mig.rw.Unlock()
	if !mig.stor.SelfLeader() {
		return storage.ErrSlaveNoSupport
	}
	namespaces, err := mig.stor.ListNamespace()
	if err != nil {
		return err
	}
	var doingTasks []*etcd.MigrateTask
	for _, namespace :=range namespaces {
		clusters, err := mig.stor.ListCluster(namespace)
		if err != nil {
			return err
		}
		for _, cluster :=range clusters {
			tasks , err := mig.stor.GetMigrateTasks(namespace, cluster)
			if err != nil {
				return err
			}
			if len(tasks) > 0 {
				mig.tasks[NsClusterName(namespace, cluster)] = tasks
			}
			doing, err := mig.stor.GetMigrateTaskDoing(namespace, cluster)
			if err != nil {
				return err
			}
			has, err := mig.stor.HasMigrateTaskHistory(doing)
			if err != nil {
				return err
			}
			if !has && doing != nil {
				doingTasks = append(doingTasks, doing)
				tasks = mig.tasks[NsClusterName(namespace, cluster)]
				mig.tasks[NsClusterName(namespace, cluster)] = append([]*etcd.MigrateTask{doing}, tasks...)
			} else if len(tasks) > 0 {
				doingTasks = append(doingTasks, tasks[0])
			}
		}
	}
	go mig.loop()

	var wg sync.WaitGroup
	go func() {
		wg.Add(1)
		for _, doing :=range doingTasks {
			mig.notifyCh<- doing
		}
		wg.Done()
	}()
	wg.Wait()

	mig.stopCh = make(chan struct{})
	mig.ready = true
	return nil
}

// AddMigrateTasks push tasks to queue
func (mig *Migrate) AddMigrateTasks(tasks []*etcd.MigrateTask) error {
	if !mig.Ready() {
		return ErrMigrateNotReady
	}
	if len(tasks) == 0 {
		return ErrAddMigTasksEmpty
	}
	namespace := tasks[0].Namespace
	cluster := tasks[0].Cluster
	taskID := tasks[0].TaskID
	for _, task := range tasks {
		if namespace != task.Namespace {
			return ErrAddMigTasksNsMistmatch
		}
		if cluster != task.Cluster {
			return ErrAddMigTasksClusterMistmatch
		}
		if taskID != task.TaskID {
			return ErrAddMigTasksIDMistmatch
		}
		// MigrateSlot ascending sort and no overlap
		sort.Slice(task.MigrateSlot, func(i, j int) bool {
			return task.MigrateSlot[i].Start < task.MigrateSlot[j].Start
		})
		task.SlotDoing = -1
		task.Status = TaskPending
		task.PendingTime = time.Now().Unix()
	}
	has, err := mig.stor.HasMigrateTask(tasks[0].Namespace, tasks[0].Cluster, tasks[0].TaskID) 
	if err != nil {
		return err
	}
	if has {
		return ErrAddMigTasksHasExisted
	}
	if err := mig.pushTask(namespace, cluster, tasks); err != nil {
		return err
	}
	mig.notifyCh <- tasks[0]
	return nil
}

// GetMigrateTasks return tasks by type, support `pending, doing, done`
func (mig *Migrate) GetMigrateTasks(namespace, cluster string, historyType string)([]*etcd.MigrateTask, error) {
	if !mig.Ready() {
		return nil, ErrMigrateNotReady
	}
	if !mig.stor.SelfLeader() {
		return nil, storage.ErrSlaveNoSupport
	}
	name := NsClusterName(namespace, cluster)
	switch historyType {
      case "pengding": 
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
      case "done" : 
      	return mig.stor.GetMigrateTaskHistory(namespace, cluster)
    }
    return nil, ErrGetMigTasksTypeMistmatch
}

// Ready return an indicator whether the migrate can work
func (mig *Migrate) Ready() bool{
	mig.rw.RLock()
	defer mig.rw.RUnlock()
	return mig.ready
}

// loop wait tasks come, groupby `namespace/cluster`
func (mig *Migrate) loop() {
	for {
		if !mig.stor.SelfLeader() {
			time.Sleep(time.Duration(etcd.SessionTTL) * time.Second)
			continue
		}
		select {
		case task := <- mig.notifyCh:
			if mig.hasDoing(task.Namespace, task.Cluster) {
				continue
			}
			go mig.migrateDoing(task.Namespace, task.Cluster)
		case <- mig.stopCh:
			return 
		case <- mig.quitCh:
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
		case <- mig.stopCh:
			return
		default:
		}
		task := mig.popTask(namespace, cluster)
		if task == nil {
			return
		}
		if err := mig.addDoing(task); err != nil {
			mig.abortTask(task, err, nil)
			continue
		}
		sourceNode, err := mig.stor.GetMasterNode(namespace, cluster, task.Source)
		if err != nil {
			mig.abortTask(task, err, nil)
			continue
		}
		targetNode, err := mig.stor.GetMasterNode(namespace, cluster, task.Target)
		if err != nil {
			mig.abortTask(task, err, nil)
			continue
		}
		cli, err := util.RedisPool(sourceNode.Address)
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
				// task.SlotDoing = slot
				mig.stor.UpdateMigrateTaskDoing(task)
				err := mig.migrateDoingSlot(cli, task, &sourceNode, &targetNode, slot, firstMigrate)
				firstMigrate = false
				if err == nil {
					continue
				} 
				switch err.Error() {
				case ErrAbortSlot.Error():
				case ErrAbortTask.Error():
					goto loop
				case ErrAbortMigrate.Error():
					return
				default:
					mig.abortTask(task, err, cli)
					goto loop
				}
			}
		}
		mig.finishTask(task, cli)
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
			return ErrAbortTask
		}
		if clusterInfo.MigratingSlot == task.SlotDoing && clusterInfo.MigratingState == MigrateSlotSuccess {
			if err := mig.stor.MigrateSlot(task.Namespace, task.Cluster, task.Source, task.Target, task.SlotDoing); err != nil {
		    	mig.abortTask(task, err, cli)
				return ErrAbortTask
			}
			return ErrAbortSlot
		}
	}
	task.SlotDoing = slot

	has, err := mig.stor.HasSlot(task.Namespace, task.Cluster, task.Source, slot)
	if err != nil {
		mig.abortTask(task, err, cli)
		return ErrAbortTask
	}
	if !has {
		mig.abortTask(task, ErrMigrateSlotNoExists, cli)
		return ErrAbortTask
	}

	err = cli.Do(context.Background(), "CLUSTERX", "migrate", strconv.Itoa(slot), target.ID).Err()
	if err != nil {
		switch err.Error() {
		case ErrMigrateSlotCompleted.Error():
			if err := mig.stor.MigrateSlot(task.Namespace, task.Cluster, task.Source, task.Target, slot); err != nil {
		    	mig.abortTask(task, err, cli)
				return ErrAbortTask
			}
			return ErrAbortSlot
		case ErrMigrateSlotDoing.Error():
		default:
			mig.abortTask(task, err, cli)
			return ErrAbortTask
		}
	}

	count := 0
	checkResultTicker := time.NewTicker(time.Duration(MigrateTaskCheckInterval) * time.Second)
	for {
		if count == MigrateTaskCheckMaxCount / MigrateTaskCheckInterval {
			mig.abortTask(task, ErrMigrateTaskTimeout, cli)
			return ErrAbortTask
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
				mig.abortTask(task, ErrMigrateSlotMismatch, cli)
				return ErrAbortTask
			}
			switch clusterInfo.MigratingState {
			case MigrateSlotFail:
				mig.abortTask(task, ErrMigrateSlotFail, cli)
				return ErrAbortTask
			case MigrateSlotSuccess:
				if err := mig.stor.MigrateSlot(task.Namespace, task.Cluster, task.Source, task.Target, task.SlotDoing); err != nil {
			    	mig.abortTask(task, err, cli)
					return ErrAbortTask
				}
				return nil
			}
		case <- mig.stopCh:
			return ErrAbortMigrate
		case <-mig.quitCh:
			return ErrAbortMigrate
		}
	}
	return ErrAbortTask
}