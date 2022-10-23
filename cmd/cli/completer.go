package main

import (
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
		options := optionCompleter(args, strings.HasPrefix(w, "--"))
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
			return nil
		}
	default:
		if len(args) == 1 {
			return prompt.FilterContains(operations, args[0], true)
		}
	}
	return nil
}
