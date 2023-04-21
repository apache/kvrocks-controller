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
	"fmt"
	"os"
	"strings"

	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/jedib0t/go-pretty/v6/table"
)

func PrintError(err error) {
	fmt.Println("Error:", err)
}

func PrintStrings(strings []string) {
	for _, str := range strings {
		fmt.Println(str)
	}
}

func PrintStatus(status string) {
	fmt.Println(status)
}

func slotRangesToString(slotRanges []metadata.SlotRange) string {
	slotBuilder := strings.Builder{}
	for j, slotRange := range slotRanges {
		slotBuilder.WriteString(slotRange.String())
		if j != len(slotRanges)-1 {
			slotBuilder.WriteByte(' ')
		}
	}
	return slotBuilder.String()
}

func PrintCluster(cluster *metadata.Cluster) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"#", "Slots", "Master", "Slaves", "Import", "Migrate"})
	for i, shard := range cluster.Shards {
		var master string
		slaves := make([]string, 0)
		for _, node := range shard.Nodes {
			if node.IsMaster() {
				master = node.Addr
			} else {
				slaves = append(slaves, node.Addr)
			}
		}
		t.AppendRows([]table.Row{
			{i, slotRangesToString(shard.SlotRanges), master, slaves, shard.ImportSlot, shard.MigratingSlot},
		})
	}
	t.Render()
}

func PrintShard(shard *metadata.Shard) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"ID", "IP:Port", "Role", "Slots", "Import", "Migrate"})
	for _, node := range shard.Nodes {
		t.AppendRows([]table.Row{
			{
				node.ID,
				node.Addr,
				node.Role,
				slotRangesToString(shard.SlotRanges),
				shard.ImportSlot,
				shard.MigratingSlot,
			},
		})
	}
	t.Render()
}
