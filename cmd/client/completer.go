package main

import (
	"strconv"
	"strings"
	"unicode"

	"github.com/apache/kvrocks-controller/metadata"

	"github.com/c-bata/go-prompt"
)

type Completer struct {
	promptCtx *PromptContext
	request   *Request
}

const (
	// Commands
	commandExit   = "exit"
	commandEnter  = "cd"
	commandCreate = "create"
	commandDelete = "rm"
	commandList   = "ls"

	// Options
	optionNodes    = "--nodes"
	optionReplicas = "--replicas"
	optionPassword = "--password"

	// Resource Types
	typeNamespace = "namespace"
	typeCluster   = "cluster"
	typeShard     = "shard"
)

var commands = []prompt.Suggest{
	{Text: commandExit, Description: "Exit the program"},
	{Text: commandEnter, Description: "Enter a namespace, cluster or shard"},
	{Text: commandCreate, Description: "Create a namespace, cluster or shard"},
	{Text: commandDelete, Description: "Delete a namespace, cluster or shard"},
	{Text: commandList, Description: "List namespace, cluster or shard"},
}

var options = []prompt.Suggest{
	{Text: optionReplicas, Description: "Replica count, default is 1"},
	{Text: optionNodes, Description: "Node list, separated by comma"},
	{Text: optionPassword, Description: "Node's password, all nodes should have the same password"},
}

func NewCompleter(promptCtx *PromptContext, request *Request) *Completer {
	return &Completer{
		request:   request,
		promptCtx: promptCtx,
	}
}

func GetWords(text string) []string {
	return strings.Fields(text)
}

func (c *Completer) IsSpaceOrTab(d prompt.Document) bool {
	text := d.TextBeforeCursor()
	if len(text) == 0 {
		return false
	}
	return unicode.IsSpace(rune(text[len(text)-1]))
}

func (c *Completer) CompleteOptions(d prompt.Document) []prompt.Suggest {
	words := GetWords(d.TextBeforeCursor())
	lastWord := words[len(words)-1]
	return prompt.FilterContains(options, lastWord, true)
}

func (c *Completer) CompleteResourceType(d prompt.Document) []prompt.Suggest {
	words := GetWords(d.TextBeforeCursor())
	if len(words) > 3 {
		return nil
	}
	prefix := ""
	if len(words) == 2 {
		prefix = words[1]
	}
	state := c.promptCtx.state
	switch state {

	case promptStateRoot:
		return prompt.FilterContains([]prompt.Suggest{{Text: typeNamespace}}, prefix, true)
	case promptStateNamespace:
		return prompt.FilterContains([]prompt.Suggest{{Text: typeCluster}}, prefix, true)
	case promptStateCluster:
		return prompt.FilterContains([]prompt.Suggest{{Text: typeShard}}, prefix, true)
	default:
		return nil
	}
}

func (c *Completer) CompleteResource(d prompt.Document) []prompt.Suggest {
	var err error
	var promptTexts []string

	words := GetWords(d.TextBeforeCursor())
	if len(words) > 3 {
		return nil
	}
	prefix := ""
	if len(words) == 2 {
		prefix = words[1]
	}
	state := c.promptCtx.state
	if state == promptStateRoot {
		promptTexts, err = c.request.ListNamespace()
	} else if state == promptStateNamespace {
		ns := c.promptCtx.namespace
		promptTexts, err = c.request.ListCluster(ns)
	} else if state == promptStateCluster {
		ns := c.promptCtx.namespace
		cluster := c.promptCtx.cluster
		var clusterInfo *metadata.Cluster
		clusterInfo, err = c.request.GetCluster(ns, cluster)
		if clusterInfo != nil {
			for i := 0; i < len(clusterInfo.Shards); i++ {
				promptTexts = append(promptTexts, strconv.Itoa(i))
			}
		}
	} else if state == promptStateShard {
		promptTexts = []string{}
	}
	if err != nil {
		return nil
	}

	if state != promptStateRoot {
		promptTexts = append(promptTexts, "..")
	}
	suggestions := make([]prompt.Suggest, len(promptTexts))
	for i, text := range promptTexts {
		suggestions[i] = prompt.Suggest{Text: text}
	}
	return prompt.FilterContains(suggestions, prefix, true)
}

func (c *Completer) Complete(d prompt.Document) []prompt.Suggest {
	words := GetWords(d.TextBeforeCursor())
	wordCnt := len(words)

	if wordCnt == 0 {
		return commands
	}
	// Should prompt commands if the last character is space or tab.
	if wordCnt == 1 && !c.IsSpaceOrTab(d) {
		return prompt.FilterContains(commands, words[0], true)
	}
	state := c.promptCtx.state
	if state == promptStateRoot && wordCnt > 2 {
		return nil
	}

	command := words[0]
	switch strings.ToLower(command) {
	case commandEnter, commandDelete:
		return c.CompleteResource(d)
	case commandCreate:
		if wordCnt == 1 && c.IsSpaceOrTab(d) || wordCnt == 2 && !c.IsSpaceOrTab(d) {
			return c.CompleteResourceType(d)
		}
		if wordCnt == 3 && c.IsSpaceOrTab(d) ||
			wordCnt >= 4 && strings.HasPrefix(words[3], "-") {
			return c.CompleteOptions(d)
		}
	}
	return nil
}
