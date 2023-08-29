package main

import "errors"

var (
	ErrUnknownCommand    = errors.New("unknown command")
	ErrWrongArguments    = errors.New("wrong arguments")
	ErrNamespaceNotExits = errors.New("namespace does not exist")
	ErrClusterNotExits   = errors.New("cluster does not exist")
)
