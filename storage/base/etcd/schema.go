package etcd

import (
	"strconv"
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
 * 		`/${ns}/${cl}/failover/doing:${FiloverTask-json}`
 * 		`/${ns}/${cl}/failover/history/${timestamp}/${nodeid}:${FiloverTask-json}`
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

	// MigrateTaskKeyPrefix mark cluster migrate tasks data
	MigrateTaskKeyPrefix = "/migrate/tasks/"

	// MigrateDoingKeyPrefix mark cluster migrate doing task data
	MigrateDoingKeyPrefix = "/migrate/doing"

	// MigrateHistoryKeyPrefix mark cluster migrate history task data
	MigrateHistoryKeyPrefix = "/migrate/history/"

	// FailoverDoingKeyPrefix mark cluster failover doing task data
	FailoverDoingKeyPrefix = "/failover/doing"

	// FailoverHistoryKeyPrefix mark cluster failover history task data
	FailoverHistoryKeyPrefix = "/failover/history/"
)

var (
	// SessionTTL leader lease timeout
	// SessionTTL * 2/3 renew lease timeout
	SessionTTL = 15

	// MonitorSleep leader observe time interval
	MonitorSleep = 1

	// EtcdDailTimeout dail etcd timeout
	EtcdDailTimeout = 5

	// EtcdTimeout etcd request timeout
	EtcdTimeout = 3 * time.Second
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

// NsClusterMigrateTaskKey return /${ns}/${cl}/migrate/tasks/${taskid}/${subid}
func NsClusterMigrateTaskKey(ns, cluster string, taskID, subID uint64) string {
	return Delimiter + ns + Delimiter + cluster + MigrateTaskKeyPrefix + strconv.FormatUint(taskID, 10) + Delimiter + strconv.FormatUint(subID, 10)
}

// NsClusterMigrateTaskKeyPrefix return /${ns}/${cl}/migrate/tasks/
func NsClusterMigrateTaskKeyPrefix(ns, cluster string) string {
	return Delimiter + ns + Delimiter + cluster + MigrateTaskKeyPrefix
}

// NsClusterMigrateTaskIDPrefix return /${ns}/${cl}/migrate/task/${taskid}/
func NsClusterMigrateTaskIDPrefix(ns, cluster string, taskID uint64) string {
	return Delimiter + ns + Delimiter + cluster + MigrateTaskKeyPrefix + strconv.FormatUint(taskID, 10)
}

// NsClusterMigrateDoingKey return /${ns}/${cl}/migrate/doing
func NsClusterMigrateDoingKey(ns, cluster string) string {
	return Delimiter + ns + Delimiter + cluster + MigrateDoingKeyPrefix
}

// NsClusterMigrateHistoryKey return /${ns}/${cl}/migrate/history/${taskid}/${subid}
func NsClusterMigrateHistoryKey(ns, cluster string, taskID, subID uint64) string {
	return Delimiter + ns + Delimiter + cluster + MigrateHistoryKeyPrefix + strconv.FormatUint(taskID, 10) + Delimiter + strconv.FormatUint(subID, 10)
}

// NsClusterMigrateHistoryPrefix return /${ns}/${cl}/migrate/history/
func NsClusterMigrateHistoryPrefix(ns, cluster string) string {
	return Delimiter + ns + Delimiter + cluster + MigrateHistoryKeyPrefix
}

// NsClusterMigrateHistoryTaskIDPrefix return /${ns}/${cl}/migrate/history/${taskid}/
func NsClusterMigrateHistoryTaskIDPrefix(ns, cluster string, taskID uint64) string {
	return Delimiter + ns + Delimiter + cluster + MigrateHistoryKeyPrefix + strconv.FormatUint(taskID, 10)
}

// NsClusterFailoverDoingKey return /${ns}/${cl}/failover/doing
func NsClusterFailoverDoingKey(ns, cluster string) string {
	return Delimiter + ns + Delimiter + cluster + FailoverDoingKeyPrefix
}

// NsClusterFailoverHistoryKey return /${ns}/${cl}/failover/historty/${timestamp}/${nodeid}
func NsClusterFailoverHistoryKey(ns, cluster, node string, ts int64) string {
	return Delimiter + ns + Delimiter + cluster + FailoverHistoryKeyPrefix + strconv.FormatInt(ts, 10) + Delimiter + node
}

// NsClusterFailoverHistoryKey return /${ns}/${cl}/failover/historty/
func NsClusterFailoverHistoryPrefix(ns, cluster string) string {
	return Delimiter + ns + Delimiter + cluster + FailoverHistoryKeyPrefix
}
