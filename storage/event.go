package storage

type EventType int
type Command int

const (
	EventNamespace EventType = iota + 1
	EventCluster
	EventShard
	EventNode
)

const (
	CommandCreate = iota + 1
	CommandRemove
	CommandUpdate
	CommandAddSlots
	CommandRemoveSlots
)

type Event struct {
	Namespace string
	Cluster   string
	Shard     int
	NodeID    string
	Type      EventType
	Command   Command
	Data      interface{}
}
