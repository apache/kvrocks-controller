package storage

import "github.com/KvrocksLabs/kvrocks-controller/metadata"

type Storage interface {
	Open() error
	BecomeLeader(id string) (bool, <-chan struct{})
	Notify() <-chan Event
	Close() error

	GetCluster(namespace, cluster string) (*metadata.Cluster, error)
}
