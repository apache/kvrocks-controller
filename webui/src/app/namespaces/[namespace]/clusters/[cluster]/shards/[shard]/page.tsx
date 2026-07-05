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
 */

"use client";

import { use, useCallback, useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { Button, Chip } from "@mui/material";
import DnsIcon from "@mui/icons-material/Dns";
import DeviceHubIcon from "@mui/icons-material/DeviceHub";
import CheckCircleIcon from "@mui/icons-material/CheckCircle";
import AlarmIcon from "@mui/icons-material/Alarm";
import SwapHorizIcon from "@mui/icons-material/SwapHoriz";

import { ShardSidebar } from "@/app/ui/sidebar";
import { deleteNode, fetchShard } from "@/app/lib/api";
import { LoadingSpinner } from "@/app/ui/loadingSpinner";
import { truncateText } from "@/app/utils";
import EmptyState from "@/app/ui/emptyState";
import { NodeCreation } from "@/app/ui/formCreation";
import { FailoverDialog } from "@/app/ui/failoverDialog";
import {
    FilterListIcon,
    FilterSortMenu,
    PageHeader,
    PageShell,
    ResourceRow,
    SearchInput,
    SortIcon,
    StatCard,
} from "@/app/ui/pageChrome";

type FilterOption = "all" | "master" | "replica";
type SortOption = "index-asc" | "index-desc" | "uptime-desc" | "uptime-asc";

const calculateUptime = (timestamp: number) => {
    const now = Math.floor(Date.now() / 1000);
    const seconds = now - timestamp;
    if (seconds < 60) return `${seconds}s`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
    if (seconds < 86400) return `${Math.floor(seconds / 3600)}h`;
    return `${Math.floor(seconds / 86400)}d`;
};

export default function ShardPage(props: {
    params: Promise<{ namespace: string; cluster: string; shard: string }>;
}) {
    const params = use(props.params);
    const { namespace, cluster, shard } = params;
    const [nodesData, setNodesData] = useState<any>(null);
    const [loading, setLoading] = useState(true);
    const [deleting, setDeleting] = useState<number | null>(null);
    const [search, setSearch] = useState("");
    const [filter, setFilter] = useState<FilterOption>("all");
    const [sort, setSort] = useState<SortOption>("index-asc");
    const [failoverOpen, setFailoverOpen] = useState(false);
    const router = useRouter();

    const refresh = useCallback(async () => {
        try {
            const fetched = await fetchShard(namespace, cluster, shard);
            if (!fetched) {
                router.push("/404");
                return;
            }
            setNodesData(fetched);
        } catch (error) {
            console.error("Error fetching shard:", error);
        }
    }, [namespace, cluster, shard, router]);

    useEffect(() => {
        (async () => {
            setLoading(true);
            await refresh();
            setLoading(false);
        })();
    }, [refresh]);

    const nodes = useMemo(() => (nodesData?.nodes as any[]) || [], [nodesData]);
    const hasReplicas = nodes.some((n) => n.role === "slave");

    const filtered = useMemo(
        () =>
            nodes
                .map((node, idx) => ({ ...node, __index: idx }))
                .filter((node) => {
                    if (!`node ${node.__index + 1}`.toLowerCase().includes(search.toLowerCase()))
                        return false;
                    if (filter === "master") return node.role === "master";
                    if (filter === "replica") return node.role !== "master";
                    return true;
                })
                .sort((a, b) => {
                    switch (sort) {
                        case "index-asc":
                            return a.__index - b.__index;
                        case "index-desc":
                            return b.__index - a.__index;
                        case "uptime-desc":
                            return b.created_at - a.created_at;
                        case "uptime-asc":
                            return a.created_at - b.created_at;
                    }
                }),
        [nodes, search, filter, sort]
    );

    const handleDelete = async (nodeId: string, index: number) => {
        if (!confirm(`Delete Node ${index + 1}? This cannot be undone.`)) return;
        try {
            setDeleting(index);
            const res = await deleteNode(namespace, cluster, shard, nodeId);
            if (res) {
                alert(`Failed to delete node: ${res}`);
                return;
            }
            await refresh();
        } catch (e) {
            alert(`Failed to delete node: ${e}`);
        } finally {
            setDeleting(null);
        }
    };

    if (loading) return <LoadingSpinner />;

    const masterCount = nodes.filter((n) => n.role === "master").length;
    const replicaCount = nodes.length - masterCount;

    return (
        <PageShell sidebar={<ShardSidebar namespace={namespace} cluster={cluster} />}>
            <PageHeader
                icon={<DnsIcon sx={{ fontSize: 16 }} />}
                title={`Shard ${parseInt(shard) + 1}`}
                subtitle={`${cluster} · ${namespace}`}
                actions={
                    <>
                        <SearchInput
                            value={search}
                            onChange={setSearch}
                            placeholder="Search nodes…"
                        />
                        <Button
                            variant="outlined"
                            size="small"
                            startIcon={<SwapHorizIcon sx={{ fontSize: 13 }} />}
                            onClick={() => setFailoverOpen(true)}
                            disabled={!hasReplicas}
                        >
                            Failover
                        </Button>
                        <NodeCreation
                            position="page"
                            namespace={namespace}
                            cluster={cluster}
                            shard={shard}
                        />
                    </>
                }
            />

            <div className="px-8 py-8 md:px-10 md:py-10">
                <div className="mb-8 grid grid-cols-2 gap-4 sm:grid-cols-3">
                    <StatCard
                        label="Nodes"
                        value={nodes.length}
                        icon={<DeviceHubIcon sx={{ fontSize: 20 }} />}
                        accent="warning"
                    />
                    <StatCard
                        label="Master"
                        value={masterCount}
                        icon={<CheckCircleIcon sx={{ fontSize: 20 }} />}
                        accent="success"
                    />
                    <StatCard
                        label="Replicas"
                        value={replicaCount}
                        icon={<DeviceHubIcon sx={{ fontSize: 20 }} />}
                        accent="info"
                    />
                </div>

                <div className="overflow-hidden rounded-xl border border-border-subtle bg-surface-base shadow-subtle dark:border-border-dark-subtle dark:bg-surface-dark-subtle">
                    <div className="flex items-center justify-between border-b border-border-subtle px-6 py-3.5 dark:border-border-dark-subtle">
                        <div className="lin-eyebrow">All nodes</div>
                        <div className="flex items-center gap-1">
                            <FilterSortMenu
                                ariaLabel="Filter"
                                tooltip="Filter"
                                icon={<FilterListIcon sx={{ fontSize: 17 }} />}
                                value={filter}
                                onChange={setFilter}
                                options={[
                                    { value: "all", label: `All (${nodes.length})` },
                                    { value: "master", label: `Master (${masterCount})` },
                                    { value: "replica", label: `Replicas (${replicaCount})` },
                                ]}
                            />
                            <FilterSortMenu
                                ariaLabel="Sort"
                                tooltip="Sort"
                                icon={<SortIcon sx={{ fontSize: 17 }} />}
                                value={sort}
                                onChange={setSort}
                                options={[
                                    { value: "index-asc", label: "Index 1 → N", group: "Index" },
                                    { value: "index-desc", label: "Index N → 1", group: "Index" },
                                    { value: "uptime-desc", label: "Newest", group: "Uptime" },
                                    { value: "uptime-asc", label: "Oldest", group: "Uptime" },
                                ]}
                            />
                        </div>
                    </div>

                    {filtered.length > 0 ? (
                        <ul className="divide-y divide-border-subtle dark:divide-border-dark-subtle">
                            {filtered.map((node) => (
                                <li key={node.__index}>
                                    <ResourceRow
                                        icon={<DeviceHubIcon sx={{ fontSize: 18 }} />}
                                        title={`Node ${node.__index + 1}`}
                                        subtitle={
                                            <span className="flex items-center gap-3 font-mono text-2xs">
                                                <span>{truncateText(node.id, 10)}</span>
                                                <span>·</span>
                                                <span className="font-sans not-italic">
                                                    {node.addr}
                                                </span>
                                            </span>
                                        }
                                        badges={
                                            node.role === "master" ? (
                                                <span className="flex items-center gap-1 rounded-md border border-success/40 bg-success/10 px-1.5 py-0.5 text-2xs font-medium text-success">
                                                    <span className="h-1 w-1 rounded-full bg-success" />
                                                    Master
                                                </span>
                                            ) : (
                                                <span className="flex items-center gap-1 rounded-md border border-info/40 bg-info/10 px-1.5 py-0.5 text-2xs font-medium text-info">
                                                    <span className="h-1 w-1 rounded-full bg-info" />
                                                    Replica
                                                </span>
                                            )
                                        }
                                        meta={
                                            <span className="flex items-center gap-1 text-xs text-text-muted dark:text-text-dark-muted">
                                                <AlarmIcon sx={{ fontSize: 12 }} />
                                                {calculateUptime(node.created_at)}
                                            </span>
                                        }
                                        href={`/namespaces/${namespace}/clusters/${cluster}/shards/${shard}/nodes/${node.__index}`}
                                        onDelete={() => handleDelete(node.id, node.__index)}
                                        deleteDisabled={deleting === node.__index}
                                    />
                                </li>
                            ))}
                        </ul>
                    ) : (
                        <div className="p-12">
                            <EmptyState
                                title={filter !== "all" ? "No matching nodes" : "No nodes yet"}
                                description={
                                    filter !== "all"
                                        ? "Change the filter to see more results."
                                        : search
                                          ? "Try a different search term."
                                          : "Create a node to get started."
                                }
                                icon={<DeviceHubIcon sx={{ fontSize: 24 }} />}
                            />
                        </div>
                    )}

                    {filtered.length > 0 && (
                        <div className="border-t border-border-subtle px-6 py-3 text-xs text-text-muted dark:border-border-dark-subtle dark:text-text-dark-muted">
                            Showing {filtered.length} of {nodes.length}
                            {filter !== "all" && " (filtered)"}
                        </div>
                    )}
                </div>
            </div>

            <FailoverDialog
                open={failoverOpen}
                onClose={() => setFailoverOpen(false)}
                namespace={namespace}
                cluster={cluster}
                shard={shard}
                nodes={nodes}
                onSuccess={refresh}
            />
        </PageShell>
    );
}
