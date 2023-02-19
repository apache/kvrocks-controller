package main

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/TwiN/go-color"
)

func randString(length int) string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	table := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	builder := strings.Builder{}
	for i := 0; i < length; i++ {
		builder.WriteByte(table[rand.Intn(62)])
	}
	return builder.String()
}

func Info(format string, args ...interface{}) {
	fmt.Println(color.Ize(color.Bold, fmt.Sprintf(format, args...)))
}

func Error(format string, args ...interface{}) {
	fmt.Println(color.Ize(color.Red, fmt.Sprintf(format, args...)))
}
