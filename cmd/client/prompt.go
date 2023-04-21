/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

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
