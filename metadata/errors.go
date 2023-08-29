package metadata

import (
	"errors"
)

var (
	ErrEntryNoExists           = errors.New("the entry does not exist")
	ErrEntryExisted            = errors.New("the entry already existed")
	ErrIndexOutOfRange         = errors.New("index out of range")
	ErrShardNoMatchPromoteNode = errors.New("no match promote node in shard")
	ErrShardNoReplica          = errors.New("no replica in shard")
)
