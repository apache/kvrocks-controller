package storage

import (
	context2 "context"
	"errors"

	"github.com/KvrocksLabs/kvrocks_controller/storage/persistence/etcd"
)

func (s *Storage) UpdateFailOverTask(task *etcd.FailOverTask) error {
	s.rw.Lock()
	defer s.rw.Unlock()

	if task == nil {
		return errors.New("nil fail over task")
	}
	return s.instance.UpdateFailOverTask(context2.Background(), task)
}

func (s *Storage) GetFailOverTask(ns, cluster string) (*etcd.FailOverTask, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()

	return s.instance.GetFailOverTask(context2.Background(), ns, cluster)
}

func (s *Storage) AddFailOverHistory(task *etcd.FailOverTask) error {
	s.rw.Lock()
	defer s.rw.Unlock()

	return s.instance.AddFailOverHistory(context2.Background(), task)
}

func (s *Storage) GetFailOverHistory(ns, cluster string) ([]*etcd.FailOverTask, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()

	return s.instance.GetFailOverHistory(context2.Background(), ns, cluster)
}
