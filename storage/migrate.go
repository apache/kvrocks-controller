package storage

import (
	"errors"

	"github.com/KvrocksLabs/kvrocks_controller/storage/persistence/etcd"
)

var (
	errNilMigrateTask = errors.New("nil migrate task")
)

func (s *Storage) AddMigrateTask(ns, cluster string, tasks []*etcd.MigrateTask) error {
	s.rw.Lock()
	defer s.rw.Unlock()
	if !s.isLeaderAndReady() {
		return ErrSlaveNoSupport
	}
	if len(tasks) == 0 {
		return errNilMigrateTask
	}
	return s.remote.AddMigrateTask(ns, cluster, tasks)
}

func (s *Storage) RemoveMigrateTask(task *etcd.MigrateTask) error {
	s.rw.Lock()
	defer s.rw.Unlock()
	if !s.isLeaderAndReady() {
		return ErrSlaveNoSupport
	}
	if task == nil {
		return errNilMigrateTask
	}
	return s.remote.RemoveMigrateTask(task)
}

func (s *Storage) GetMigrateTasks(ns, cluster string) ([]*etcd.MigrateTask, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()
	if !s.isLeaderAndReady() {
		return nil, ErrSlaveNoSupport
	}
	return s.remote.GetMigrateTasks(ns, cluster)
}

func (s *Storage) AddDoingMigrateTask(task *etcd.MigrateTask) error {
	s.rw.Lock()
	defer s.rw.Unlock()
	if !s.isLeaderAndReady() {
		return ErrSlaveNoSupport
	}
	if task == nil {
		return errNilMigrateTask
	}
	return s.remote.AddDoingMigrateTask(task)
}

func (s *Storage) GetDoingMigrateTask(ns, cluster string) (*etcd.MigrateTask, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()
	if !s.isLeaderAndReady() {
		return nil, ErrSlaveNoSupport
	}
	return s.remote.GetDoingMigrateTask(ns, cluster)
}

func (s *Storage) AddHistoryMigrateTask(task *etcd.MigrateTask) error {
	s.rw.Lock()
	defer s.rw.Unlock()
	if !s.isLeaderAndReady() {
		return ErrSlaveNoSupport
	}
	if task == nil {
		return errors.New("nil history migrate task")
	}
	return s.remote.AddHistoryMigrateTask(task)
}

func (s *Storage) GetHistoryMigrateTask(ns, cluster string) ([]*etcd.MigrateTask, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()
	if !s.isLeaderAndReady() {
		return nil, ErrSlaveNoSupport
	}
	return s.remote.GetHistoryMigrateTask(ns, cluster)
}

func (s *Storage) IsMigrateTaskExists(ns, cluster string, taskID uint64) (bool, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()
	if !s.isLeaderAndReady() {
		return false, ErrSlaveNoSupport
	}
	return s.remote.IsMigrateTaskExists(ns, cluster, taskID)
}

func (s *Storage) IsHistoryMigrateTaskExists(task *etcd.MigrateTask) (bool, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()
	if !s.isLeaderAndReady() {
		return false, ErrSlaveNoSupport
	}
	if task == nil {
		return false, nil
	}
	return s.remote.IsHistoryMigrateTaskExists(task)
}
