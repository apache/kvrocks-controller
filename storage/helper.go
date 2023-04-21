/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package storage

import (
	"fmt"
)

const NamespacePrefix = "/namespace"

func appendNamespacePrefix(ns string) string {
	return NamespacePrefix + "/" + ns
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

func buildFailOverKey(ns, cluster string) string {
	return fmt.Sprintf("/%s/%s/failover/queue", ns, cluster)
}

func buildFailOverHistoryKey(ns, cluster, node string, ts int64) string {
	return fmt.Sprintf("/%s/%s/failover/history/%d/%s", ns, cluster, ts, node)
}

func buildFailOverHistoryPrefix(ns, cluster string) string {
	return fmt.Sprintf("/%s/%s/failover/history/", ns, cluster)
}
