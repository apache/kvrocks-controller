package migrate

import (
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/KvrocksLabs/kvrocks_controller/logger"
	"github.com/KvrocksLabs/kvrocks_controller/metrics"
	"github.com/KvrocksLabs/kvrocks_controller/storage/base/etcd"
	"github.com/KvrocksLabs/kvrocks_controller/util"
)

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
func (mig *Migrate) abortTask(task *etcd.MigrateTask, err error, cli *redis.Client) {
	task.Status = TaskFail
	task.Err = err.Error()
	task.DoneTime = time.Now().Unix()
	mig.storage.AddHistoryMigrateTask(task)
	mig.removeDoingTaskFromMemory(task)
	logger.Get().With(
		zap.Error(err),
		zap.Any("task", task),
	).Error("Abort migrate task")
	metrics.PrometheusMetrics.AllNodes.With(
		prometheus.Labels{"namespace": task.Namespace,
			"cluster": task.Cluster}).Inc()
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
