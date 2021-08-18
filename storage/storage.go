package storage

type Storage interface {
	Open() error
	BecomeLeader(id string) (bool, <-chan struct{})
	Notify() <-chan Event
	Close() error
}
