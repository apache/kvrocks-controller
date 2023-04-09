package metadata

import (
	"errors"
)

var (
	ErrEntryNoExists   = errors.New("the entry does not exist")
	ErrEntryExisted    = errors.New("the entry already existed")
	ErrIndexOutOfRange = errors.New("index out of range")
)
