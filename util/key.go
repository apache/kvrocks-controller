package util

import (
	"fmt"
)

func BuildClusterKey(ns, cluster string) string {
	return fmt.Sprintf("%s/%s", ns, cluster)
}
