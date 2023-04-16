package main

import (
	"strings"

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
	optionNodes    = "nodes"
	optionReplica  = "replica"
	optionPassword = "password"
)

var commands = []prompt.Suggest{
	{Text: commandExit, Description: "Exit the program"},
	{Text: commandEnter, Description: "Enter a namespace, cluster or shard"},
	{Text: commandCreate, Description: "Create a namespace, cluster or shard"},
	{Text: commandDelete, Description: "Delete a namespace, cluster or shard"},
	{Text: commandList, Description: "List namespace, cluster or shard"},
}

var options = []prompt.Suggest{
	{Text: optionReplica, Description: "Replica count, default is 1"},
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
	return d.LastKeyStroke() == prompt.Tab || d.LastKeyStroke() == prompt.ControlSpace
}

func (c *Completer) CompleteOptions(d prompt.Document) []prompt.Suggest {
	words := GetWords(d.TextBeforeCursor())
	lastWord := words[len(words)-1]
	optionPrefix := ""
	if strings.HasPrefix(lastWord, "--") {
		optionPrefix = lastWord[2:]
	} else {
		optionPrefix = lastWord[1:]
	}
	return prompt.FilterContains(options, optionPrefix, true)
}

func (c *Completer) CompleteResource(d prompt.Document) []prompt.Suggest {
	var err error
	var promptTexts []string

	words := GetWords(d.TextBeforeCursor())
	if len(words) > 3 {
		return nil
	}
	state := c.promptCtx.state
	if state == promptStateRoot {
		promptTexts, err = c.request.ListNamespace()
	} else if state == promptStateNamespace {
		ns := c.promptCtx.namespace
		promptTexts, err = c.request.ListCluster(ns)
	}
	if err != nil {
		return nil
	}
	suggestions := make([]prompt.Suggest, len(promptTexts))
	for i, ns := range promptTexts {
		suggestions[i] = prompt.Suggest{Text: ns}
	}
	return prompt.FilterContains(suggestions, words[1], true)
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
		if wordCnt == 2 && c.IsSpaceOrTab(d) ||
			wordCnt == 3 && strings.HasPrefix(words[2], "-") {
			return c.CompleteOptions(d)
		}
	}
	return nil
}
