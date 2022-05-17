package migrate

import (
	"fmt"
	"time"
	"errors"
	"sync"
	"strconv"
	"context"

	"go.uber.org/zap"
	"github.com/go-redis/redis/v8"
	"github.com/KvrocksLabs/kvrocks-controller/logger"
	"github.com/KvrocksLabs/kvrocks-controller/storage"
	"github.com/KvrocksLabs/kvrocks-controller/util"
	"github.com/KvrocksLabs/kvrocks-controller/storage/base/etcd"
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
	
)

var (
	// MigrateTaskCheckInterval minute check kvrocks-node migrate status
	MigrateTaskCheckInterval = 1 

	// MigrateTaskCheckMaxCount * MigrateTaskCheckInterval migrate timeout 
	MigrateTaskCheckMaxCount = 24 * 60

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

// Migrate implement tasks queue and doing task in memory
// schedule tasks and interact migrate storage(etcd)
type Migrate struct {
	stor  *storage.Storage
	tasks map[string][]*etcd.MigrateTask // memory tasks queue, group by `namespace/cluster`
	doing map[string]*etcd.MigrateTask   // doing task, group by `namespace/cluster`

	notifyCh  chan *etcd.MigrateTask     // notify when push tasks queue 
	quitCh    chan struct{}
	rw        sync.RWMutex
}

// NewMigrate creates a Migrate, need call Load to schedule tasks
func NewMigrate(stor  *storage.Storage) (*Migrate, error) {
	migrate := &Migrate{
		stor:     stor,
		tasks:    make(map[string][]*etcd.MigrateTask),
		doing:    make(map[string]*etcd.MigrateTask),
		notifyCh: make(chan *etcd.MigrateTask, 10),
		quitCh:   make(chan struct{}),
	}
	return migrate, nil
}

// Close call by quit or leader-follower switch
func (mig *Migrate) Close() error {
	mig.rw.Lock()
	defer mig.rw.Unlock()
	close(mig.quitCh)
	close(mig.notifyCh)
	return nil
}

// Load tasks from migrate storage and begin schedule tasks
func (mig *Migrate) Load() error {
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
			} else if len(tasks) > 0 {
				doingTasks = append(doingTasks, tasks[0])
			}
		}
	}
	go func() {
		for _, doing :=range doingTasks {
			mig.notifyCh<- doing
		}
	}()
	go mig.loop()
	return nil
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
		case <- mig.quitCh:
			return 
		default:
		}
	}
}

// AddMigrateTasks push tasks to queue
func (mig *Migrate) AddMigrateTasks(tasks []*etcd.MigrateTask) error {
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

// abortTask handler task status and push etcd when task exception
func (mig *Migrate) abortTask(task *etcd.MigrateTask, err error) {
	task.Status = TaskFail
	task.Err = err.Error()
	mig.stor.AddMigrateTaskHistory(task)
	mig.removeDoing(task)
	logger.Get().With(zap.Error(err),).Error("task abort!!!")
	logger.Get().Info(fmt.Sprintf("migrate task failed: %v", *task))
}

// finishTask handler task status and push etcd when task success
func (mig *Migrate) finishTask(task *etcd.MigrateTask) {
	task.Status = TaskSuccess
	mig.stor.AddMigrateTaskHistory(task)
	mig.removeDoing(task)
	logger.Get().Info(fmt.Sprintf("migrate task success: %v", *task))
}

// hasTasks return an indicator whether `namespace/cluster` has tasks
func (mig *Migrate) hasTasks(namespace, cluster string) bool {
	mig.rw.RLock()
	defer mig.rw.RUnlock()
	_, ok := mig.tasks[NsClusterName(namespace, cluster)]
	return ok
}

// pushTask add tasks to queue, include memory and etcd
func (mig *Migrate) pushTask(namespace, cluster string, tasks []*etcd.MigrateTask) error {
	mig.rw.Lock()
	defer mig.rw.Unlock()
	if err := mig.stor.PushMigrateTask(namespace, cluster, tasks); err != nil {
		return err
	}
	name := NsClusterName(namespace, cluster)
	mig.tasks[name] = append(mig.tasks[name], tasks...)
	return nil
}

// popTask remove task from queue, include memory and etcd
func (mig *Migrate) popTask(namespace, cluster string) (*etcd.MigrateTask) {
	mig.rw.Lock()
	defer mig.rw.Unlock()
	name := NsClusterName(namespace, cluster)
	tasks, ok := mig.tasks[name]
	if !ok {
		return nil
	}
	task := tasks[0]
	if err := mig.stor.PopMigrateTask(task); err!= nil {
		logger.Get().With(zap.Error(err),).Error("pop migrate task from etcd failed!")
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
	_, ok := mig.doing[NsClusterName(namespace, cluster)]
	return ok
}

// addDoing schedule task to doing, update memory and etcd
func (mig *Migrate) addDoing(task *etcd.MigrateTask) error{
	mig.rw.Lock()
	defer mig.rw.Unlock()
	task.Status = TaskDoing
	task.DoingTime = time.Now().Unix()
	if err := mig.stor.UpdateMigrateTaskDoing(task); err != nil {
		return err
	}
	mig.doing[NsClusterName(task.Namespace, task.Cluster)] = task
	return nil
}

// removeDoing only delete doing task in memory
func (mig *Migrate) removeDoing(task *etcd.MigrateTask) {
	mig.rw.Lock()
	defer mig.rw.Unlock()
	task.DoneTime = time.Now().Unix()
	delete(mig.doing, NsClusterName(task.Namespace, task.Cluster))
	return 
}

// AbortMigrateSlotError return an indicator whether error can not ignore
func (mig *Migrate) AbortMigrateSlotError(err error) bool {
	switch err.Error() {
	case ErrMigrateSlotDoing.Error():
		return false
	case ErrMigrateSlotCompleted.Error():
		return false
	}
	return true
}

// NsClusterName return `namespace/cluster`
func NsClusterName(ns, cluster string) string {
	return ns + etcd.Delimiter + cluster
}

// migrateDoing do tasks by slot
func (mig *Migrate) migrateDoing(namespace, cluster string) {
begin:
	for {
		select {
		case <-mig.quitCh:
			return 
		default:
		}
		task := mig.popTask(namespace, cluster)
		if task == nil {
			return
		}
		if err := mig.addDoing(task); err != nil {
			mig.abortTask(task, err)
			continue
		}
		sourceNode, err := mig.stor.GetMasterNode(namespace, cluster, task.Source)
		if err != nil {
			mig.abortTask(task, err)
			continue
		}
		targetNode, err := mig.stor.GetMasterNode(namespace, cluster, task.Target)
		if err != nil {
			mig.abortTask(task, err)
			continue
		}
		cli := redis.NewClient(&redis.Options{
			Addr: sourceNode.Address,
		})
		for _, slotRange := range task.MigrateSlot {
			for slot := slotRange.Start; slot <= slotRange.Stop; slot++ {
				task.SlotDoing = slot
				mig.stor.UpdateMigrateTaskDoing(task)
				has, err := mig.stor.HasSlot(namespace, cluster, task.Source, slot)
				if err != nil {
					mig.abortTask(task, err)
					cli.Close()
					goto begin
				}
				if !has {
					mig.abortTask(task, ErrMigrateSlotNoExists)
					cli.Close()
					goto begin
				}
				err = cli.Do(context.Background(), "CLUSTERX", "migrate", strconv.Itoa(slot), targetNode.ID).Err()
				if err != nil {
					if mig.AbortMigrateSlotError(err) {
						mig.abortTask(task, err)
						cli.Close()
						goto begin
					}
					if err.Error() == ErrMigrateSlotCompleted.Error() {
						continue
					}
				}
				count := 0
				checkResultTicker := time.NewTicker(time.Duration(MigrateTaskCheckInterval) * time.Minute)
				for {
					if count == MigrateTaskCheckMaxCount {
						mig.abortTask(task, ErrMigrateTaskTimeout)
						cli.Close()
						goto begin
					}
					select {
					case <-checkResultTicker.C:
						count++
						clusterInfo, err := util.ClusterInfoCmd(sourceNode.Address)
						if err != nil {
							logger.Get().With(
					    		zap.Error(err),
					    	).Error("slot which has been migrated")
							continue
						}
						if clusterInfo.MigratingSlot != task.SlotDoing {
							mig.abortTask(task, ErrMigrateSlotMismatch)
							cli.Close()
							goto begin
						}
						if clusterInfo.MigratingState == MigrateSlotFail {
							mig.abortTask(task, ErrMigrateSlotFail)
							cli.Close()
							goto begin
						}
						if clusterInfo.MigratingState == MigrateSlotSuccess {
							if err := mig.stor.MigrateSlot(task.Namespace, task.Cluster, task.Source, task.Target, slot); err != nil {
						    	mig.abortTask(task, err)
						    	cli.Close()
								goto begin
							}
							goto slotDone
						}
					case <-mig.quitCh:
						cli.Close()
						return 
					default:
					}
				}
slotDone:
			}
		}
		cli.Close()
		mig.finishTask(task)
	}
}