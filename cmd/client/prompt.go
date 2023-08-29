package main

import "fmt"

const (
	promptStateRoot = iota + 1
	promptStateNamespace
	promptStateCluster
	promptStateShard
)

type PromptContext struct {
	state     int
	namespace string
	cluster   string
	shard     int
}

func NewPromptContext() *PromptContext {
	return &PromptContext{state: promptStateRoot}
}

func (ctx *PromptContext) Reset() {
	ctx.state = promptStateRoot
	ctx.namespace = ""
	ctx.cluster = ""
	ctx.shard = -1
}

func (ctx *PromptContext) SetNamespace(ns string) {
	ctx.state = promptStateNamespace
	ctx.namespace = ns
	ctx.cluster = ""
	ctx.shard = -1
}

func (ctx *PromptContext) SetCluster(cluster string) {
	ctx.state = promptStateCluster
	ctx.cluster = cluster
	ctx.shard = -1
}

func (ctx *PromptContext) SetShard(shard int) {
	ctx.state = promptStateShard
	ctx.shard = shard
}

func (ctx *PromptContext) Prefix() (string, bool) {
	switch ctx.state {
	case promptStateNamespace:
		return fmt.Sprintf("%s>> ", ctx.namespace), true
	case promptStateCluster:
		return fmt.Sprintf("%s/%s>> ", ctx.namespace, ctx.cluster), true
	case promptStateShard:
		return fmt.Sprintf("%s/%s/%d>> ", ctx.namespace, ctx.cluster, ctx.shard), true
	default:
		return ">> ", true
	}
}
