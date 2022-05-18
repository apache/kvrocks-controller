package storage

import (
	"errors"

	"github.com/KvrocksLabs/kvrocks-controller/storage/base/etcd"
)

// PushMigrateTask push migrate task to queue back
func (stor *Storage) PushMigrateTask(ns, cluster string, tasks []*etcd.MigrateTask) error {
	stor.rw.Lock()
	defer stor.rw.Unlock()
	if !stor.selfLeaderReady() {
		return ErrSlaveNoSupport
	}
	if len(tasks) == 0 {
		return errors.New("push migrate task is empty")
	}
	return stor.remote.PushMigrateTask(ns, cluster, tasks)
}

// PopMigrateTask pop migrate task from queue front
func (stor *Storage) PopMigrateTask(task *etcd.MigrateTask) error {
	stor.rw.Lock()
	defer stor.rw.Unlock()
	if !stor.selfLeaderReady() {
		return ErrSlaveNoSupport
	}
	if task == nil {
		return errors.New("pop migrate task is nil")
	}
	return stor.remote.PopMigrateTask(task)
}

// GetMigrateTasks return migrate tasks
func (stor *Storage) GetMigrateTasks(ns, cluster string)([]*etcd.MigrateTask, error){
	stor.rw.RLock()
	defer stor.rw.RUnlock()
	if !stor.selfLeaderReady() {
		return nil, ErrSlaveNoSupport
	}
	return stor.remote.GetMigrateTasks(ns, cluster)
}

// UpdateMigrateTaskDoing update doing maigrate task info
func (stor *Storage) UpdateMigrateTaskDoing(task *etcd.MigrateTask) error {
	stor.rw.Lock()
	defer stor.rw.Unlock()
	if !stor.selfLeaderReady() {
		return ErrSlaveNoSupport
	}
	if task == nil {
		return errors.New("update migrate task doing is nil")
	}
	return stor.remote.UpdateMigrateTaskDoing(task)
}	


// GetMigrateTaskDoing return doing maigrate task info
func (stor *Storage) GetMigrateTaskDoing(ns, cluster string)(*etcd.MigrateTask, error) {
	stor.rw.RLock()
	defer stor.rw.RUnlock()
	if !stor.selfLeaderReady() {
		return nil, ErrSlaveNoSupport
	}
	return stor.remote.GetMigrateTaskDoing(ns, cluster)
}


// AddMigrateTaskHistory add maigrate task to history record
func (stor *Storage) AddMigrateTaskHistory(task *etcd.MigrateTask) error {
	stor.rw.Lock()
	defer stor.rw.Unlock()
	if !stor.selfLeaderReady() {
		return ErrSlaveNoSupport
	}
	if task == nil {
		return errors.New("add migrate task history is nil")
	}
	return stor.remote.AddMigrateTaskHistory(task)
}

// GetMigrateTaskHistory return the list of maigrate tasks of history records
func (stor *Storage) GetMigrateTaskHistory(ns, cluster string)([]*etcd.MigrateTask, error) {
	stor.rw.RLock()
	defer stor.rw.RUnlock()
	if !stor.selfLeaderReady() {
		return nil, ErrSlaveNoSupport
	}
	return stor.remote.GetMigrateTaskHistory(ns, cluster)
}

// HasMigrateTask return an indicator whether the cluster have the maigrate task
func (stor *Storage) HasMigrateTask(ns, cluster string, taskID uint64)(bool, error) {
	stor.rw.RLock()
	defer stor.rw.RUnlock()
	if !stor.selfLeaderReady() {
		return false, ErrSlaveNoSupport
	}
	return stor.remote.HasMigrateTask(ns, cluster, taskID)
}

// HasMigrateTaskHistory return an indicator whether the cluster have the maigrate task is history
func (stor *Storage) HasMigrateTaskHistory(task *etcd.MigrateTask)(bool, error) {
	stor.rw.RLock()
	defer stor.rw.RUnlock()
	if !stor.selfLeaderReady() {
		return false, ErrSlaveNoSupport
	}
	if task == nil {
		return false , nil
	}
	return stor.remote.HasMigrateTaskHistory(task)
}