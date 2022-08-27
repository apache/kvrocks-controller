package storage

import (
	"errors"

	"github.com/KvrocksLabs/kvrocks_controller/storage/base/etcd"
)

func (s *Storage) UpdateDoingFailOverTask(task *etcd.FailOverTask) error {
	s.rw.Lock()
	defer s.rw.Unlock()
	if !s.isLeaderAndReady() {
		return ErrSlaveNoSupport
	}
	if task == nil {
		return errors.New("nil fail over task")
	}
	return s.remote.UpdateDoingFailOverTask(task)
}

func (s *Storage) GetDoingFailOverTask(ns, cluster string) (*etcd.FailOverTask, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()
	if !s.isLeaderAndReady() {
		return nil, ErrSlaveNoSupport
	}
	return s.remote.GetDoingFailOverTask(ns, cluster)
}

func (s *Storage) AddFailOverHistory(task *etcd.FailOverTask) error {
	s.rw.Lock()
	defer s.rw.Unlock()
	if !s.isLeaderAndReady() {
		return ErrSlaveNoSupport
	}
	return s.remote.AddFailOverHistory(task)
}

func (s *Storage) GetFailOverHistory(ns, cluster string) ([]*etcd.FailOverTask, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()
	if !s.isLeaderAndReady() {
		return nil, ErrSlaveNoSupport
	}
	return s.remote.GetFailOverHistory(ns, cluster)
}
