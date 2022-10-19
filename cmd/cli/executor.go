package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/KvrocksLabs/kvrocks_controller/cmd/cli/client"

	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/server/handlers"
	"github.com/olekukonko/tablewriter"
)

type Executor struct {
	client *client.Client
}

func NewExecutor(c *client.Client) *Executor {
	return &Executor{
		client: c,
	}
}

func (e *Executor) CreateNamespace(ctx context.Context, options *resourceOptions) {
	if err := e.client.CreateNamespace(ctx, options.Namespace); err != nil {
		Error("failed to create namespace: %v\n", err)
		return
	}
	Info("created")
}

func (e *Executor) createCluster(ctx context.Context, options *resourceOptions) {
	nodeCnt := len(options.Nodes)
	replica := options.Replica
	if options.Replica == 0 {
		replica = 1
	}
	if nodeCnt < replica || nodeCnt%replica != 0 {
		Error("this is not possible with %d nodes and %d replicas per node.", nodeCnt, replica)
		return
	}
	if options.Cluster == "" {
		Error("missing or incorrect '--cluster' parameter")
	}
	if len(options.Nodes) == 0 {
		Error("missing or incorrect '--nodes' parameter")
	}

	req := handlers.CreateClusterRequest{
		Cluster: options.Cluster,
	}
	for i := 0; i < nodeCnt/replica; i++ {
		shard := handlers.CreateShardRequest{
			Master: &metadata.NodeInfo{
				ID:      randString(40),
				Address: options.Nodes[i*replica],
			},
		}
		for j := 1; j < replica; j++ {
			shard.Slaves = append(shard.Slaves, metadata.NodeInfo{
				ID:      randString(40),
				Address: options.Nodes[i*replica+j],
			})
		}
		req.Shards = append(req.Shards, shard)
	}
	if err := e.client.CreateCluster(ctx, options.Namespace, &req); err != nil {
		Error("failed to create cluster: %v\n", err)
		return
	}
	Info("created")
}

func (e *Executor) createResource(resource string, args []string) {
	options, err := parseOptions(args)
	if err != nil {
		Error("failed to parse option: %v\n", err)
		return
	}

	ctx := context.Background()
	if options.Namespace == "" {
		Error("you must use `--namespace` to specify the namespace\n")
		return
	}
	switch strings.ToLower(resource) {
	case resourceNamespace:
		e.CreateNamespace(ctx, options)
	case resourceCluster:
		e.createCluster(ctx, options)
	}
}

func (e *Executor) ListResource(resource string, args []string) {
	options, err := parseOptions(args)
	if err != nil {
		Error("parse options: %v\n", err)
		return
	}

	ctx := context.Background()
	switch strings.ToLower(resource) {
	case resourceNamespace:
		namespaces, err := e.client.ListNamespace(ctx)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		if len(namespaces) == 0 {
			Error("no namespace")
			return
		}
		table := tablewriter.NewWriter(os.Stdout)
		table.SetRowLine(true)
		table.SetRowSeparator("-")
		for _, namespace := range namespaces {
			table.Append([]string{namespace})
		}
		table.Render()
	case resourceCluster:
		clusters, err := e.client.ListCluster(ctx, options.Namespace)
		if err != nil {
			Error("list cluster: %v\n", err)
			return
		}
		if len(clusters) == 0 {
			Error("no cluster")
			return
		}
		table := tablewriter.NewWriter(os.Stdout)
		table.SetRowLine(true)
		table.SetRowSeparator("-")
		for _, cluster := range clusters {
			table.Append([]string{cluster})
		}
		table.Render()
	case resourceShard:
		cluster, err := e.client.GetCluster(ctx, options.Namespace, options.Cluster)
		if err != nil {
			Error("list shard: %v\n", err)
			return
		}
		if len(cluster.Shards) == 0 {
			fmt.Println("no shard")
			return
		}
		table := tablewriter.NewWriter(os.Stdout)
		table.SetRowLine(true)
		table.SetRowSeparator("-")
		table.SetHeader([]string{"Index", "Slots", "Master", "Slaves", "Importing", "Migrating"})
		for i, shard := range cluster.Shards {
			slots := make([]string, 0)
			for _, slotRange := range shard.SlotRanges {
				slots = append(slots, slotRange.String())
			}
			var masterAddr string
			slaveAddrs := make([]string, 0)
			for _, node := range shard.Nodes {
				if node.Role == "master" {
					masterAddr = node.Address
					continue
				}
				slaveAddrs = append(slaveAddrs, node.Address)
			}
			table.Append([]string{
				strconv.Itoa(i),
				strings.Join(slots, ","), masterAddr,
				strings.Join(slaveAddrs, ","),
				strconv.Itoa(shard.ImportSlot),
				strconv.Itoa(shard.MigratingSlot),
			})
		}
		table.Render()
	}
}

func (e *Executor) Run(s string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return
	} else if s == "quit" || s == "exit" {
		Info("bye bye!")
		os.Exit(0)
		return
	}
	args := strings.Fields(strings.TrimSpace(s))
	switch strings.ToLower(args[0]) {
	case operationList:
		if len(args) > 1 {
			e.ListResource(args[1], args[2:])
		}
		// TODO: show helper
	case operationCreate:
		if len(args) > 1 {
			e.createResource(args[1], args[2:])
		}
		// TODO: show helper
	default:
		Error("unsupported command: %s", args[0])
	}
	return
}
