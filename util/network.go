package util

import (
	"net"
	"strconv"
	"strings"
)

func IsIPPort(s string) bool {
	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return false
	}
	return IsIP(parts[0]) && IsPort(parts[1])
}

func IsIP(ip string) bool {
	return net.ParseIP(ip) != nil
}

func IsPort(port string) bool {
	p, err := strconv.Atoi(port)
	if err != nil {
		return false
	}
	return p > 0 && p < 65536
}
