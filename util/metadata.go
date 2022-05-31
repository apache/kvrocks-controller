package util

import (
	"github.com/KvrocksLabs/kvrocks_controller/storage/base/etcd"
)

func NsClusterJoin(ns, cluster string) string {
	return ns + etcd.Delimiter + cluster
}
