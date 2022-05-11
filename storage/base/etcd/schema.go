package etcd

import (
	"time"
)

/*
 * etcd data format desgin principle: 
 * 1. continuous storage among cluster data(topo and meta) under the same namespace
 * 2. minimize multiple etcd accesses for one upper-layer operation
 *
 * 	namespace format:
 * 		`/namespace/${ns}:${ns}`
 * 	all namespace name has the same prefix `/namespace/`, convenient for list namespace
 *
 *  cluster topo format:
 * 		`/${ns}/cluster/${cl}:${Clsuter-json}` 	
 *	all clusters(topo and meta) hava the same prefix /${ns}/
 *  all clusters topo data hava the same prefix /${ns}/cluster/
 *
 * 	migrate metedata format:
 * 		`/${ns}/${cl}/migrate/tasks/${timestamp}_${sub_id}:${MigrateTask-json}`
 * 		`/${ns}/${cl}/migrate/doing:${MigrateTask-json}`
 * 		`/${ns}/${cl}/migrate/history/${timestamp}_${sub_id}:${MigrateTask-json}`
 * 	all migrate metedata under specified cluster hava the same prefix /${ns}/${cl}/migrate/
 *
 * 	failover metedata format:
 * 		`/${ns}/${cl}/failover/tasks/${nodeid}:${timestamp}`
 * 		`/${ns}/${cl}/failover/doing:${FiloverTask-json}`
 * 		`/${ns}/${cl}/failover/history/${timestamp}_${nodeid}:${FiloverTask-json}`
 *  all failover metedata under specified cluster hava the same prefix /${ns}/${cl}/failover/
 */
var (
	// Delimiter is tool for etcd data format
	Delimiter = "/"

	// LeaderKey use for leader election and leader observe
	LeaderKey = "kvrocks_controller_leader"

	// NamespaceKeyPrefix fixed prefix for access namespaces
	NamespaceKeyPrefix = "/namespace/"

	// ClusterKeyPrefix mark cluster topo data, distinguish cluster meta data
	ClusterKeyPrefix = "/cluster/"
)

var (
	// SessionTTL leader lease timeout 
	// SessionTTL * 2/3 renew lease timeout
	SessionTTL   = 15 
	
	// MonitorSleep leader observe time interval
	MonitorSleep = 1
	
	// EtcdTimeout etcd request timeout
	EtcdTimeout  = 3 * time.Second
)

// NamespaceKey return /namespace/${ns}
func NamespaceKey(ns string) string {
	return NamespaceKeyPrefix + ns
}

// NsClusterPrefixKey return /${ns}/cluster/
func NsClusterPrefixKey(ns string) string {
	return Delimiter + ns + ClusterKeyPrefix
}

// NsClusterKey return /${ns}/cluster/${cl}
func NsClusterKey(ns, cluster string) string {
	return Delimiter + ns + ClusterKeyPrefix + cluster
}

// NsClusterMetaKey return /${ns}/${cl} , include migrate and failover meta key
func NsClusterMetaKey(ns, cluster string) string {
	return Delimiter + ns + Delimiter + cluster
}