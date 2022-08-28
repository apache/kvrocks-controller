package etcd

import (
	"fmt"
	"time"
)

var (
	LeaderKey          = "kvrocks-controller-leader"
	NamespaceKeyPrefix = "/namespace/"
)

var (
	SessionTTL   = 15
	MonitorSleep = 1

	defaultDailTimeout = 5 * time.Second
	defaultTimeout     = 3 * time.Second
)

func appendNamespacePrefix(ns string) string {
	return NamespaceKeyPrefix + ns
}

func buildClusterPrefix(ns string) string {
	return fmt.Sprintf("/%s/cluster", ns)
}

func buildClusterKey(ns, cluster string) string {
	return fmt.Sprintf("/%s/cluster/%s", ns, cluster)
}

func buildClusterMetaKey(ns, cluster string) string {
	return fmt.Sprintf("/%s/%s", ns, cluster)
}

func buildMigrateTaskKey(ns, cluster string, taskID, subID uint64) string {
	return fmt.Sprintf("/%s/%s/migrate/tasks/%d/%d", ns, cluster, taskID, subID)
}

func buildMigrateTaskKeyPrefix(ns, cluster string) string {
	return fmt.Sprintf("/%s/%s/migrate/tasks/", ns, cluster)
}

func buildMigrateTaskIDPrefix(ns, cluster string, taskID uint64) string {
	return fmt.Sprintf("/%s/%s/migrate/tasks/%d", ns, cluster, taskID)
}

func buildMigratingKeyPrefix(ns, cluster string) string {
	return fmt.Sprintf("/%s/%s/migrate/doing", ns, cluster)
}

func buildMigrateHistoryKey(ns, cluster string, taskID, subID uint64) string {
	return fmt.Sprintf("/%s/%s/migrate/history/%d/%d", ns, cluster, taskID, subID)
}

func buildMigrateHistoryPrefix(ns, cluster string) string {
	return fmt.Sprintf("/%s/%s/migrate/history/", ns, cluster)
}

func buildMigrateHistoryTaskPrefix(ns, cluster string, taskID uint64) string {
	return fmt.Sprintf("/%s/%s/migrate/history/%d", ns, cluster, taskID)
}

func buildDoingFailOverKey(ns, cluster string) string {
	return fmt.Sprintf("/%s/%s/failover/doing", ns, cluster)
}

func buildFailOverHistoryKey(ns, cluster, node string, ts int64) string {
	return fmt.Sprintf("/%s/%s/failover/history/%d/%s", ns, cluster, ts, node)
}

func buildFailOverHistoryPrefix(ns, cluster string) string {
	return fmt.Sprintf("/%s/%s/failover/history/", ns, cluster)
}
