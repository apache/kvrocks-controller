package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_create(t *testing.T) {
	request := NewRequest(config.Endpoint)
	promptCtx := NewPromptContext()
	executor := NewExecutor(promptCtx, request)

	executor.promptCtx.state = promptStateNamespace
	words := [...]string{"create", "cluster"}
	require.EqualError(t, executor.create(words[:]), ErrWrongArguments.Error())
}
