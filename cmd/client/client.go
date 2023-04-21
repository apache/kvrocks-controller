package main

import (
	"flag"
	"os"

	"github.com/c-bata/go-prompt"
)

var config struct {
	Endpoint string
}

func init() {
	flag.StringVar(&config.Endpoint, "e", "", "set Kvrocks controller server endpoint")
}

func main() {
	flag.Parse()

	if len(config.Endpoint) == 0 {
		config.Endpoint = "http://127.0.0.1:9379"
	}
	promptCtx := NewPromptContext()
	request := NewRequest(config.Endpoint)
	completer := NewCompleter(promptCtx, request)
	executor := NewExecutor(promptCtx, request)
	executorFunc := func(input string) {
		if quit := executor.Execute(input); quit {
			os.Exit(0)
		}
	}

	p := prompt.New(
		executorFunc,
		completer.Complete,
		prompt.OptionPrefix(">>"),
		prompt.OptionLivePrefix(promptCtx.Prefix),
		prompt.OptionTitle("kvctl"),
	)
	p.Run()
}
