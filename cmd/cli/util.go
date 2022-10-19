package main

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/TwiN/go-color"
)

func randString(length int) string {
	r := rand.New(rand.NewSource(time.Now().UnixMicro()))
	table := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	builder := strings.Builder{}
	for i := 0; i < length; i++ {
		builder.WriteByte(table[r.Intn(62)])
	}
	return builder.String()
}

func Info(format string, args ...interface{}) {
	fmt.Println(color.Ize(color.Bold, fmt.Sprintf(format, args...)))
}

func Error(format string, args ...interface{}) {
	fmt.Println(color.Ize(color.Red, fmt.Sprintf(format, args...)))
}
