package storage

import (
	context2 "context"
	"errors"

	"github.com/KvrocksLabs/kvrocks_controller/storage/persistence/etcd"
)

var (
	errNilMigrateTask = errors.New("nil migrate task")
)

func (s *Storage) AddPendingMigrateTask(ns, cluster string, tasks []*etcd.MigrateTask) error {
	s.rw.Lock()
	defer s.rw.Unlock()

	if len(tasks) == 0 {
		return errNilMigrateTask
	}
	return s.instance.AddPendingMigrateTask(context2.Background(), ns, cluster, tasks)
}

func (s *Storage) RemovePendingMigrateTask(task *etcd.MigrateTask) error {
	s.rw.Lock()
	defer s.rw.Unlock()

	if task == nil {
		return errNilMigrateTask
	}
	return s.instance.RemovePendingMigrateTask(context2.Background(), task)
}

func (s *Storage) GetPendingMigrateTasks(ns, cluster string) ([]*etcd.MigrateTask, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()

	return s.instance.GetPendingMigrateTasks(context2.Background(), ns, cluster)
}

func (s *Storage) AddMigrateTask(task *etcd.MigrateTask) error {
	s.rw.Lock()
	defer s.rw.Unlock()

	if task == nil {
		return errNilMigrateTask
	}
	return s.instance.AddMigrateTask(context2.Background(), task)
}

func (s *Storage) ListMigrateTask(ns, cluster string) (*etcd.MigrateTask, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()

	return s.instance.GetMigrateTask(context2.Background(), ns, cluster)
}

func (s *Storage) AddMigrateHistory(task *etcd.MigrateTask) error {
	s.rw.Lock()
	defer s.rw.Unlock()

	if task == nil {
		return errors.New("nil history migrate task")
	}
	return s.instance.AddMigrateHistory(context2.Background(), task)
}

func (s *Storage) GetMigrateHistory(ns, cluster string) ([]*etcd.MigrateTask, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()

	return s.instance.GetMigrateHistory(context2.Background(), ns, cluster)
}

func (s *Storage) IsMigrateTaskExists(ns, cluster string, taskID uint64) (bool, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()

	return s.instance.IsMigrateTaskExists(context2.Background(), ns, cluster, taskID)
}

func (s *Storage) IsMigrateHistoryExists(task *etcd.MigrateTask) (bool, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()

	if task == nil {
		return false, nil
	}
	return s.instance.IsMigrateHistoryExists(context2.Background(), task)
}
