package main

import (
	"github.com/KvrocksLabs/kvrocks_controller/cmd/cli/client"
	"github.com/c-bata/go-prompt"
	"github.com/c-bata/go-prompt/completer"
)

func main() {
	Info("please use `exit` or `Ctrl-D` to exit this program.")
	defer Info("bye!")

	client := client.New("")
	c := NewCompleter(client)
	executor := NewExecutor(client)
	p := prompt.New(
		executor.Run,
		c.Complete,
		prompt.OptionTitle("kvprompt: interactive Kvrocks controller client"),
		prompt.OptionPrefix(">>> "),
		prompt.OptionInputTextColor(prompt.Yellow),
		prompt.OptionCompletionWordSeparator(completer.FilePathCompletionSeparator),
	)
	p.Run()
}
