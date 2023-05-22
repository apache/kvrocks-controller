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

import (
	"flag"
	"os"

	"github.com/c-bata/go-prompt"
)

var config struct {
	Endpoint string
}

func init() {
	flag.StringVar(&config.Endpoint, "e", "", "set Kvrocks controller server endpoint")
}

func main() {
	flag.Parse()

	envVar := os.Getenv("KVROCKS_CONTROLLER_HTTP_ADDR")
	if len(envVar) > 0 {
		config.Endpoint = envVar
	}

	if len(config.Endpoint) == 0 {
		config.Endpoint = "http://127.0.0.1:9379"
	}
	promptCtx := NewPromptContext()
	request := NewRequest(config.Endpoint)
	completer := NewCompleter(promptCtx, request)
	executor := NewExecutor(promptCtx, request)
	executorFunc := func(input string) {
		if quit := executor.Execute(input); quit {
			os.Exit(0)
		}
	}

	p := prompt.New(
		executorFunc,
		completer.Complete,
		prompt.OptionPrefix(">>"),
		prompt.OptionLivePrefix(promptCtx.Prefix),
		prompt.OptionTitle("kvctl"),
	)
	p.Run()
}
