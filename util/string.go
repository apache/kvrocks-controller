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

func IsUniqueSlice(list interface{}) bool {
	switch items := list.(type) {
	case []string:
		set := make(map[string]struct{})
		for _, item := range items {
			_, ok := set[item]
			if ok {
				return false
			}
			set[item] = struct{}{}
		}
		return true
	case []int:
		set := make(map[int]struct{})
		for _, item := range items {
			_, ok := set[item]
			if ok {
				return false
			}
			set[item] = struct{}{}
		}
		return true
	}

	panic("only support string and int")
}
