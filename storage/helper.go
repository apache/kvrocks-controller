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
