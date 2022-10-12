package metadata

import (
	"fmt"
)

var (
	ErrNamespaceNoExists   = NewError("namespace", CodeNoExists, "")
	ErrNamespaceHasExisted = NewError("namespace", CodeExisted, "")

	ErrClusterNoExists   = NewError("cluster", CodeNoExists, "")
	ErrClusterHasExisted = NewError("cluster", CodeExisted, "")

	ErrShardIndexOutOfRange = NewError("shard", CodeIndexOutOfRange, "")

	ErrNodeNoExists = NewError("node", CodeNoExists, "")

	ErrSlotNoExists = NewError("slot", CodeNoExists, "")
)

const (
	CodeExisted = iota + 1
	CodeNoExists
	CodeIndexOutOfRange
)

type Error struct {
	Module string
	Code   int
	Desc   string
}

var code2Desc = map[int]string{
	CodeNoExists:        "no exists",
	CodeExisted:         "already existed",
	CodeIndexOutOfRange: "index out of range",
}

func NewError(module string, code int, desc string) *Error {
	if desc == "" {
		desc = code2Desc[code]
	}
	return &Error{
		Module: module,
		Code:   code,
		Desc:   desc,
	}
}

func (e Error) Error() string {
	return fmt.Sprintf("module=%s, code=%d, desc=%s", e.Module, e.Code, e.Desc)
}
