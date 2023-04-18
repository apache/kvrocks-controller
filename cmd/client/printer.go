package main

import (
	"fmt"
	"os"

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

func PrintCluster(cluster *metadata.Cluster) error {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"#", "Slots", "Master", "Slaves", "Import", "Migrate"})
	for i, shard := range cluster.Shards {
		slotsString, err := shard.ToSlotsString()
		if err != nil {
			return err
		}
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
			{i, shard.Nodes[0], slotsString, master, slaves, shard.ImportSlot, shard.MigratingSlot},
		})
	}
	return nil
}
