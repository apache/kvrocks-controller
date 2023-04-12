package util

import (
	"math/rand"
	"strings"
	"time"
)

func RandString(length int) string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	table := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	builder := strings.Builder{}
	for i := 0; i < length; i++ {
		builder.WriteByte(table[r.Intn(62)])
	}
	return builder.String()
}

func GenerateNodeID() string {
	return RandString(40)
}
