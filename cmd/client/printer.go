package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/RocksLabs/kvrocks_controller/metadata"
	"github.com/RocksLabs/kvrocks_controller/util"
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
	t.AppendHeader(table.Row{"ID", "IP:Port", "Role", "Slots", "Import", "Migrate", "Status"})
	for _, node := range shard.Nodes {
		status := "Alive"
		if err := util.PingCmd(context.Background(), &node); err != nil {
			status = "Dead"
		}
		t.AppendRows([]table.Row{
			{
				node.ID,
				node.Addr,
				node.Role,
				slotRangesToString(shard.SlotRanges),
				shard.ImportSlot,
				shard.MigratingSlot,
				status,
			},
		})
	}
	t.Render()
}
