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

	operationList   = "list"
	operationCreate = "create"
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
	commands := []prompt.Suggest{
		{Text: operationCreate},
		{Text: operationList},
	}
	resources := []prompt.Suggest{
		{Text: resourceNamespace},
		{Text: resourceCluster},
		{Text: resourceShard},
	}

	args := strings.Fields(strings.TrimSpace(d.TextBeforeCursor()))
	if len(args) == 0 {
		return commands
	}
	w := d.GetWordBeforeCursor()
	// If word before the cursor starts with "-", returns CLI flag options.
	if strings.HasPrefix(w, "-") {
		return optionCompleter(args, strings.HasPrefix(w, "--"))
	}

	switch strings.ToLower(args[0]) {
	case operationCreate, operationList:
		switch len(args) {
		case 1:
			return resources
		case 2:
			return prompt.FilterContains(resources, args[1], true)
		default:
			return nil
		}
	default:
		if len(args) == 1 {
			return prompt.FilterContains(commands, args[0], true)
		}
	}
	return nil
}
