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

	if len(tasks) == 0 {
		return errNilMigrateTask
	}
	return s.instance.AddMigrateTask(ns, cluster, tasks)
}

func (s *Storage) RemoveMigrateTask(task *etcd.MigrateTask) error {
	s.rw.Lock()
	defer s.rw.Unlock()

	if task == nil {
		return errNilMigrateTask
	}
	return s.instance.RemoveMigrateTask(task)
}

func (s *Storage) GetMigrateTasks(ns, cluster string) ([]*etcd.MigrateTask, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()

	return s.instance.GetMigrateTasks(ns, cluster)
}

func (s *Storage) AddDoingMigrateTask(task *etcd.MigrateTask) error {
	s.rw.Lock()
	defer s.rw.Unlock()

	if task == nil {
		return errNilMigrateTask
	}
	return s.instance.AddDoingMigrateTask(task)
}

func (s *Storage) GetDoingMigrateTask(ns, cluster string) (*etcd.MigrateTask, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()

	return s.instance.GetDoingMigrateTask(ns, cluster)
}

func (s *Storage) AddHistoryMigrateTask(task *etcd.MigrateTask) error {
	s.rw.Lock()
	defer s.rw.Unlock()

	if task == nil {
		return errors.New("nil history migrate task")
	}
	return s.instance.AddHistoryMigrateTask(task)
}

func (s *Storage) GetHistoryMigrateTask(ns, cluster string) ([]*etcd.MigrateTask, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()

	return s.instance.GetHistoryMigrateTask(ns, cluster)
}

func (s *Storage) IsMigrateTaskExists(ns, cluster string, taskID uint64) (bool, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()

	return s.instance.IsMigrateTaskExists(ns, cluster, taskID)
}

func (s *Storage) IsHistoryMigrateTaskExists(task *etcd.MigrateTask) (bool, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()

	if task == nil {
		return false, nil
	}
	return s.instance.IsHistoryMigrateTaskExists(task)
}
