package storage

import (
	"fmt"
)

const MetadataPrefix = "/kvrocks/metadata"

func appendNamespacePrefix(ns string) string {
	return MetadataPrefix + "/" + ns
}

func buildClusterPrefix(ns string) string {
	return fmt.Sprintf("%s/%s/cluster", MetadataPrefix, ns)
}

func buildClusterKey(ns, cluster string) string {
	return fmt.Sprintf("%s/%s", buildClusterPrefix(ns), cluster)
}

func buildMigratingKeyPrefix(ns, cluster string) string {
	return fmt.Sprintf("%s/%s/migrate/doing", buildClusterPrefix(ns), cluster)
}

func buildMigrateHistoryKey(ns, cluster, taskID string) string {
	return fmt.Sprintf("%s/%s/migrate/history/%s", buildClusterPrefix(ns), cluster, taskID)
}

func buildMigrateHistoryPrefix(ns, cluster string) string {
	return fmt.Sprintf("%s/%s/migrate/history/", buildClusterPrefix(ns), cluster)
}

func buildFailOverKey(ns, cluster string) string {
	return fmt.Sprintf("%s/%s/failover/queue", buildClusterPrefix(ns), cluster)
}

func buildFailOverHistoryKey(ns, cluster, node string, ts int64) string {
	return fmt.Sprintf("%s/%s/failover/history/%d/%s", buildClusterPrefix(ns), cluster, ts, node)
}

func buildFailOverHistoryPrefix(ns, cluster string) string {
	return fmt.Sprintf("%s/%s/failover/history/", buildClusterPrefix(ns), cluster)
}
