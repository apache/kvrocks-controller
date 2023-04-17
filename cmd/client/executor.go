package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	parentDir = ".."
)

type Executor struct {
	promptCtx *PromptContext
	request   *Request
}

func NewExecutor(promptCtx *PromptContext, request *Request) *Executor {
	return &Executor{
		promptCtx: promptCtx,
		request:   request,
	}
}

func (e *Executor) Execute(input string) (quit bool) {
	if e.isQuit(input) {
		return true
	}
	words := GetWords(input)
	if len(words) == 0 {
		// do nothing
		return
	}

	var err error
	command := words[0]
	switch strings.ToLower(command) {
	case commandList:
		err = e.list(words)
	case commandCreate:
		err = e.create(words)
	case commandDelete:
		err = e.delete(words)
	case commandEnter:
		err = e.enter(words)
	default:
		err = ErrUnknownCommand
	}
	if err != nil {
		PrintError(err)
	}
	return
}

func (e *Executor) list(words []string) error {
	if len(words) > 1 {
		return ErrWrongArguments
	}
	switch e.promptCtx.state {
	case promptStateRoot:
		ns, err := e.request.ListNamespace()
		if err != nil {
			return err
		}
		PrintStrings(ns)
	case promptStateNamespace:
		ns := e.promptCtx.namespace
		clusters, err := e.request.ListCluster(ns)
		if err != nil {
			return err
		}
		PrintStrings(clusters)
	case promptStateCluster:
		// TODO: list shard
	case promptStateShard:
		// TODO: list node
	}
	return nil
}

func (e *Executor) enter(words []string) error {
	if len(words) != 2 {
		return ErrWrongArguments
	}
	switch e.promptCtx.state {
	case promptStateRoot:
		namespace := words[1]
		if namespace == parentDir {
			return errors.New("already in root")
		} else {
			exists, err := e.request.IsNamespaceExists(namespace)
			if err != nil {
				return err
			}
			if !exists {
				return ErrNamespaceNotExits
			}
			e.promptCtx.SetNamespace(namespace)
		}
		return nil
	case promptStateNamespace:
		ns := e.promptCtx.namespace
		cluster := words[1]
		if cluster == parentDir {
			e.promptCtx.Reset()
		} else {
			exists, err := e.request.IsClusterExists(ns, cluster)
			if err != nil {
				return err
			}
			if !exists {
				return ErrClusterNotExits
			}
			e.promptCtx.SetCluster(cluster)
		}
		return nil
	case promptStateCluster:
		ns := e.promptCtx.namespace
		cluster := e.promptCtx.cluster
		shard := words[1]
		if shard == parentDir {
			e.promptCtx.SetNamespace(ns)
		} else {
			shardID, err := strconv.Atoi(shard)
			if err != nil {
				return errors.New("shard id MUST be a number")
			}
			clusterInfo, err := e.request.GetCluster(ns, cluster)
			if err != nil {
				return err
			}
			if shardID < 0 || shardID >= len(clusterInfo.Shards) {
				return errors.New("shard id out of range")
			}
			e.promptCtx.SetShard(shardID)
		}
		return nil
	case promptStateShard:
		cluster := e.promptCtx.cluster
		if words[1] == parentDir {
			e.promptCtx.SetCluster(cluster)
			return nil
		}
		return errors.New("already in shard, can enter nothing")
	}
	return errors.New("unsupported enter state")
}

func parseClusterOptions(words []string) (*ClusterOptions, error) {
	if len(words) < 3 {
		return nil, ErrWrongArguments
	}

	clusterOptions := &ClusterOptions{
		Name: words[1],
	}
	for i := 2; i < len(words); i++ {
		switch words[i] {
		case "--nodes":
			if i+1 >= len(words) {
				return nil, fmt.Errorf("missing 'nodes'")
			}
			nodes := strings.Split(words[i+1], ",")
			clusterOptions.Nodes = nodes
			i++
		case "--replica":
			if i+1 >= len(words) {
				return nil, fmt.Errorf("missing 'replica'")
			}
			replica, err := strconv.Atoi(words[i+1])
			if err != nil {
				return nil, fmt.Errorf("'replica' is NOT a number")
			}
			if replica <= 0 {
				return nil, fmt.Errorf("'replica' should be greater than 0")
			}
			clusterOptions.Replica = replica
			i++
		case "--password":
			if i+1 >= len(words) {
				return nil, fmt.Errorf("missing 'password'")
			}
			clusterOptions.Password = words[i+1]
			i++
		default:
			return nil, fmt.Errorf("unknown option: %s", words[i])
		}
	}
	if len(clusterOptions.Nodes) == 0 {
		return nil, fmt.Errorf("missing 'nodes'")
	}
	if clusterOptions.Replica == 0 {
		clusterOptions.Replica = 1
	}
	if len(clusterOptions.Nodes)%clusterOptions.Replica != 0 {
		return nil, fmt.Errorf("nodes count should be divisible by replica")
	}
	return clusterOptions, nil
}

func (e *Executor) create(words []string) (err error) {
	var clusterOptions *ClusterOptions
	switch e.promptCtx.state {
	case promptStateRoot:
		if len(words) != 2 {
			return ErrWrongArguments
		}
		namespace := words[1]
		err = e.request.CreateNamespace(namespace)
	case promptStateNamespace:
		ns := e.promptCtx.namespace
		clusterOptions, err = parseClusterOptions(words)
		if err != nil {
			return err
		}
		err = e.request.CreateCluster(ns, clusterOptions)
	default:
		return errors.New("unsupported create state")
	}
	if err != nil {
		return err
	}
	PrintStatus("CREATED")
	return nil
}

func (e *Executor) delete(words []string) (err error) {
	if len(words) != 2 {
		return ErrWrongArguments
	}

	switch e.promptCtx.state {
	case promptStateRoot:
		namespace := words[1]
		err = e.request.DeleteNamespace(namespace)
	case promptStateNamespace:
		cluster := words[1]
		err = e.request.DeleteCluster(e.promptCtx.namespace, cluster)
	default:
		err = errors.New("unsupported delete state")
	}
	if err != nil {
		return err
	}
	PrintStatus("DELETED")
	return nil
}

func (e *Executor) isQuit(command string) bool {
	command = strings.TrimSpace(strings.ToLower(command))
	return command == "quit" || command == "exit"
}
