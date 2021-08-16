package meta

import (
	"fmt"
)

const (
	CodeExisted = iota + 1
	CodeNoExists
)

type Error struct {
	Module string
	Code   int
	Desc   string
}

var code2Desc = map[int]string{
	CodeNoExists: "no exists",
	CodeExisted:  "already existed",
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

func (e *Error) Error() string {
	return fmt.Sprintf("module=%s, code=%d, desc=%s", e.Module, e.Code, e.Desc)
}
