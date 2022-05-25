package migrate

import (
	"fmt"
	"time"

	"go.uber.org/zap"
	"github.com/go-redis/redis/v8"
	"github.com/KvrocksLabs/kvrocks-controller/logger"
	"github.com/KvrocksLabs/kvrocks-controller/storage/base/etcd"
	"github.com/KvrocksLabs/kvrocks-controller/util"
)

// hasTasks return an indicator whether `namespace/cluster` has tasks
func (mig *Migrate) hasTasks(namespace, cluster string) bool {
	mig.rw.RLock()
	defer mig.rw.RUnlock()
	_, ok := mig.tasks[util.NsClusterJoin(namespace, cluster)]
	return ok
}

// pushTask add tasks to queue, include memory and etcd
func (mig *Migrate) pushTask(namespace, cluster string, tasks []*etcd.MigrateTask) error {
	mig.rw.Lock()
	defer mig.rw.Unlock()
	if err := mig.stor.PushMigrateTask(namespace, cluster, tasks); err != nil {
		logger.Get().With(zap.Error(err),).Error("push migrate task to etcd failed!")
		return err
	}
	name := util.NsClusterJoin(namespace, cluster)
	mig.tasks[name] = append(mig.tasks[name], tasks...)
	return nil
}

// popTask remove task from queue, include memory and etcd
func (mig *Migrate) popTask(namespace, cluster string) (*etcd.MigrateTask) {
	mig.rw.Lock()
	defer mig.rw.Unlock()
	name := util.NsClusterJoin(namespace, cluster)
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
	_, ok := mig.doing[util.NsClusterJoin(namespace, cluster)]
	return ok
}

// addDoing schedule task to doing, update memory and etcd
func (mig *Migrate) addDoing(task *etcd.MigrateTask) error{
	mig.rw.Lock()
	defer mig.rw.Unlock()
	task.Status = TaskDoing
	task.DoingTime = time.Now().Unix()
	if err := mig.stor.UpdateMigrateTaskDoing(task); err != nil {
		logger.Get().With(zap.Error(err),).Error("pop migrate doing task to etcd failed!")
		return err
	}
	mig.doing[util.NsClusterJoin(task.Namespace, task.Cluster)] = task
	return nil
}

// removeDoing only delete doing task in memory
func (mig *Migrate) removeDoing(task *etcd.MigrateTask) {
	mig.rw.Lock()
	defer mig.rw.Unlock()
	task.DoneTime = time.Now().Unix()
	delete(mig.doing, util.NsClusterJoin(task.Namespace, task.Cluster))
	return 
}

// abortTask handler task status and push etcd when task exception
func (mig *Migrate) abortTask(task *etcd.MigrateTask, err error, cli *redis.Client) {
	task.Status = TaskFail
	task.Err = err.Error()
	task.DoneTime = time.Now().Unix()
	mig.stor.AddMigrateTaskHistory(task)
	mig.removeDoing(task)
	logger.Get().With(
		zap.Error(err),
		zap.Any("task", *task),
	).Error("task abort!!!")
}

// finishTask handler task status and push etcd when task success
func (mig *Migrate) finishTask(task *etcd.MigrateTask, cli *redis.Client) {
	task.Status = TaskSuccess
	mig.stor.AddMigrateTaskHistory(task)
	mig.removeDoing(task)
	logger.Get().Info(fmt.Sprintf("migrate task success: %v", *task))
	logger.Get().With(
		zap.Any("task", *task),
	).Info("task success!!!")
}
