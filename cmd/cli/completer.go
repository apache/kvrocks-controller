package main

import (
	"context"
	"strconv"
	"strings"

	"github.com/KvrocksLabs/kvrocks_controller/cmd/cli/client"
	"github.com/c-bata/go-prompt"
)

const (
	resourceNamespace = "namespace"
	resourceCluster   = "cluster"
	resourceShard     = "shard"
	resourceNode      = "node"
	resourceFailOver  = "failover"
	resourceMigration = "migration"

	operationList   = "list"
	operationCreate = "create"
	operationDelete = "delete"
)

type Completer struct {
	client *client.Client
}

func NewCompleter(c *client.Client) *Completer {
	return &Completer{
		client: c,
	}
}

func (c *Completer) Complete(d prompt.Document) []prompt.Suggest {
	operations := []prompt.Suggest{
		{Text: operationCreate},
		{Text: operationList},
		{Text: operationDelete},
	}
	resources := []prompt.Suggest{
		{Text: resourceNamespace},
		{Text: resourceCluster},
		{Text: resourceShard},
		{Text: resourceFailOver},
		{Text: resourceMigration},
	}

	args := strings.Fields(strings.TrimSpace(d.TextBeforeCursor()))
	if len(args) == 0 {
		return operations
	}
	w := d.GetWordBeforeCursor()
	// If word before the cursor starts with "-", returns CLI flag options.
	if strings.HasPrefix(w, "-") {
		options := c.optionCompleter(args, w)
		return prompt.FilterHasPrefix(options, w, true)
	}

	switch strings.ToLower(args[0]) {
	case operationCreate, operationList, operationDelete:
		switch len(args) {
		case 1:
			lastChar := d.Text[len(d.Text)-1]
			if lastChar == ' ' || lastChar == '\t' {
				return prompt.FilterHasPrefix(resources, args[0], true)
			}
			return prompt.FilterHasPrefix(operations, args[0], true)
		case 2:
			return prompt.FilterHasPrefix(resources, args[1], true)
		default:
			ctx := context.Background()
			switch strings.ToLower(args[len(args)-1]) {
			case "--namespace":
				suggestions := make([]prompt.Suggest, 0)
				namespaces, err := c.client.ListNamespace(ctx)
				if err != nil || len(namespaces) == 0 {
					return suggestions
				}
				for _, namespace := range namespaces {
					suggestions = append(suggestions, prompt.Suggest{Text: namespace})
				}
				return suggestions
			case "--cluster":
				suggestions := make([]prompt.Suggest, 0)
				options, err := parseOptions(args, true)
				if err != nil || options == nil || options.Namespace == "" {
					return suggestions
				}
				clusters, err := c.client.ListCluster(ctx, options.Namespace)
				if err != nil || len(clusters) == 0 {
					return suggestions
				}
				for _, cluster := range clusters {
					suggestions = append(suggestions, prompt.Suggest{Text: cluster})
				}
				return suggestions
			case "--shard":
				suggestions := make([]prompt.Suggest, 0)
				options, err := parseOptions(args, true)
				if err != nil || options == nil || options.Namespace == "" || options.Cluster == "" {
					return suggestions
				}
				cluster, err := c.client.GetCluster(ctx, options.Namespace, options.Cluster)
				if err != nil || len(cluster.Shards) == 0 {
					return suggestions
				}
				for i := 0; i < len(cluster.Shards); i++ {
					suggestions = append(suggestions, prompt.Suggest{Text: strconv.Itoa(i)})
				}
				return suggestions
			}
			return nil
		}
	default:
		if len(args) == 1 {
			return prompt.FilterContains(operations, args[0], true)
		}
	}
	return nil
}
