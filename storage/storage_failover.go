package storage

import (
	"errors"

	"github.com/KvrocksLabs/kvrocks-controller/storage/base/etcd"
)

// UpdateFailoverTaskDoing update doing failover task info
func (stor *Storage) UpdateFailoverTaskDoing(task *etcd.FailoverTask) error {
	stor.rw.Lock()
	defer stor.rw.Unlock()
	if !stor.selfLeaderReady() {
		return ErrSlaveNoSupport
	}
	if task == nil {
		return errors.New("update failover task doing is nil")
	}
	return stor.remote.UpdateFailoverTaskDoing(task)
}

// GetFailoverTaskDoing return doing failover task info
func (stor *Storage) GetFailoverTaskDoing(ns, cluster string) (*etcd.FailoverTask, error) {
	stor.rw.RLock()
	defer stor.rw.RUnlock()
	if !stor.selfLeaderReady() {
		return nil, ErrSlaveNoSupport
	}
	return stor.remote.GetFailoverTaskDoing(ns, cluster)
}

// AddFailoverHistory add failover task to history record
func (stor *Storage) AddFailoverHistory(task *etcd.FailoverTask) error {
	stor.rw.Lock()
	defer stor.rw.Unlock()
	if !stor.selfLeaderReady() {
		return ErrSlaveNoSupport
	}
	return stor.remote.AddFailoverHistory(task)
}

// GetFailoverHistory return the list of failover tasks of history records
func (stor *Storage) GetFailoverHistory(ns, cluster string) ([]*etcd.FailoverTask, error) {
	stor.rw.RLock()
	defer stor.rw.RUnlock()
	if !stor.selfLeaderReady() {
		return nil, ErrSlaveNoSupport
	}
	return stor.remote.GetFailoverHistory(ns, cluster)
}