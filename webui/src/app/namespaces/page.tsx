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

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { Chip } from "@mui/material";
import FolderIcon from "@mui/icons-material/Folder";
import FolderOpenIcon from "@mui/icons-material/FolderOpen";
import StorageIcon from "@mui/icons-material/Storage";
import DnsIcon from "@mui/icons-material/Dns";
import DeviceHubIcon from "@mui/icons-material/DeviceHub";

import { NamespaceSidebar } from "../ui/sidebar";
import { deleteNamespace, fetchClusters, fetchNamespaces, listNodes, listShards } from "../lib/api";
import { LoadingSpinner } from "../ui/loadingSpinner";
import EmptyState from "../ui/emptyState";
import {
    FilterListIcon,
    FilterSortMenu,
    PageHeader,
    PageShell,
    ResourceRow,
    SearchInput,
    SortIcon,
    StatCard,
} from "../ui/pageChrome";

interface NamespaceData {
    name: string;
    clusterCount: number;
    shardCount: number;
    nodeCount: number;
    loading: boolean;
}

type FilterOption = "all" | "with-clusters" | "no-clusters";
type SortOption =
    "name-asc" | "name-desc" | "clusters-desc" | "clusters-asc" | "nodes-desc" | "nodes-asc";

export default function Namespaces() {
    const [rows, setRows] = useState<NamespaceData[]>([]);
    const [totals, setTotals] = useState({ namespaces: 0, clusters: 0, shards: 0, nodes: 0 });
    const [loading, setLoading] = useState(true);
    const [deleting, setDeleting] = useState<string | null>(null);
    const [search, setSearch] = useState("");
    const [filter, setFilter] = useState<FilterOption>("all");
    const [sort, setSort] = useState<SortOption>("name-asc");
    const router = useRouter();

    useEffect(() => {
        let cancelled = false;

        (async () => {
            try {
                const namespaces = await fetchNamespaces();
                if (cancelled) return;

                // Render the list immediately with skeleton counts so the page never
                // blocks on the deepest fetch. Per-namespace counts populate below.
                setRows(
                    namespaces.map((name) => ({
                        name,
                        clusterCount: 0,
                        shardCount: 0,
                        nodeCount: 0,
                        loading: true,
                    }))
                );
                setTotals({ namespaces: namespaces.length, clusters: 0, shards: 0, nodes: 0 });
                setLoading(false);

                await Promise.all(
                    namespaces.map(async (namespace) => {
                        try {
                            const clusters = await fetchClusters(namespace);
                            const shardLists = await Promise.all(
                                clusters.map((cluster) => listShards(namespace, cluster))
                            );

                            let shardCount = 0;
                            const nodePromises: Promise<Object[]>[] = [];
                            clusters.forEach((cluster, ci) => {
                                const shards = shardLists[ci];
                                if (Array.isArray(shards)) {
                                    shardCount += shards.length;
                                    shards.forEach((_, i) =>
                                        nodePromises.push(
                                            listNodes(namespace, cluster, i.toString())
                                        )
                                    );
                                }
                            });

                            const nodeLists = await Promise.all(nodePromises);
                            const nodeCount = nodeLists.reduce(
                                (acc, nodes) => acc + (Array.isArray(nodes) ? nodes.length : 0),
                                0
                            );

                            if (cancelled) return;

                            setRows((prev) =>
                                prev.map((n) =>
                                    n.name === namespace
                                        ? {
                                              ...n,
                                              clusterCount: clusters.length,
                                              shardCount,
                                              nodeCount,
                                              loading: false,
                                          }
                                        : n
                                )
                            );
                            setTotals((prev) => ({
                                namespaces: prev.namespaces,
                                clusters: prev.clusters + clusters.length,
                                shards: prev.shards + shardCount,
                                nodes: prev.nodes + nodeCount,
                            }));
                        } catch (err) {
                            console.error(`Failed to load namespace ${namespace}:`, err);
                            if (!cancelled) {
                                setRows((prev) =>
                                    prev.map((n) =>
                                        n.name === namespace ? { ...n, loading: false } : n
                                    )
                                );
                            }
                        }
                    })
                );
            } catch (error) {
                console.error("Error fetching namespaces data:", error);
                if (!cancelled) setLoading(false);
            }
        })();

        return () => {
            cancelled = true;
        };
    }, [router]);

    const handleDelete = async (name: string) => {
        if (!confirm(`Delete namespace "${name}"?`)) return;
        try {
            setDeleting(name);
            await deleteNamespace(name);
            setRows((prev) => prev.filter((n) => n.name !== name));
            setTotals((prev) => ({ ...prev, namespaces: prev.namespaces - 1 }));
        } catch (e) {
            alert(`Failed to delete namespace: ${e}`);
        } finally {
            setDeleting(null);
        }
    };

    const filtered = rows
        .filter((n) => {
            if (!n.name.toLowerCase().includes(search.toLowerCase())) return false;
            if (filter === "with-clusters") return n.clusterCount > 0;
            if (filter === "no-clusters") return n.clusterCount === 0;
            return true;
        })
        .sort((a, b) => {
            switch (sort) {
                case "name-asc":
                    return a.name.localeCompare(b.name);
                case "name-desc":
                    return b.name.localeCompare(a.name);
                case "clusters-desc":
                    return b.clusterCount - a.clusterCount;
                case "clusters-asc":
                    return a.clusterCount - b.clusterCount;
                case "nodes-desc":
                    return b.nodeCount - a.nodeCount;
                case "nodes-asc":
                    return a.nodeCount - b.nodeCount;
            }
        });

    if (loading) return <LoadingSpinner />;

    return (
        <PageShell sidebar={<NamespaceSidebar />}>
            <PageHeader
                title="Namespaces"
                subtitle="Manage your Kvrocks database namespaces"
                actions={
                    <SearchInput
                        value={search}
                        onChange={setSearch}
                        placeholder="Search namespaces…"
                    />
                }
            />

            <div className="px-8 py-8 md:px-10 md:py-10">
                <div className="mb-8 grid grid-cols-2 gap-4 sm:grid-cols-4">
                    <StatCard
                        label="Namespaces"
                        value={totals.namespaces}
                        icon={<FolderIcon sx={{ fontSize: 20 }} />}
                        accent="primary"
                    />
                    <StatCard
                        label="Clusters"
                        value={totals.clusters}
                        icon={<StorageIcon sx={{ fontSize: 20 }} />}
                        accent="info"
                    />
                    <StatCard
                        label="Shards"
                        value={totals.shards}
                        icon={<DnsIcon sx={{ fontSize: 20 }} />}
                        accent="success"
                    />
                    <StatCard
                        label="Nodes"
                        value={totals.nodes}
                        icon={<DeviceHubIcon sx={{ fontSize: 20 }} />}
                        accent="warning"
                    />
                </div>

                <div className="overflow-hidden rounded-xl border border-border-subtle bg-surface-base shadow-subtle dark:border-border-dark-subtle dark:bg-surface-dark-subtle">
                    <div className="flex items-center justify-between border-b border-border-subtle px-6 py-3.5 dark:border-border-dark-subtle">
                        <div className="lin-eyebrow">All namespaces</div>
                        <div className="flex items-center gap-1">
                            <FilterSortMenu
                                ariaLabel="Filter"
                                tooltip="Filter"
                                icon={<FilterListIcon sx={{ fontSize: 15 }} />}
                                value={filter}
                                onChange={setFilter}
                                options={[
                                    { value: "all", label: `All (${rows.length})` },
                                    {
                                        value: "with-clusters",
                                        label: `With clusters (${rows.filter((r) => r.clusterCount > 0).length})`,
                                    },
                                    {
                                        value: "no-clusters",
                                        label: `Empty (${rows.filter((r) => r.clusterCount === 0).length})`,
                                    },
                                ]}
                            />
                            <FilterSortMenu
                                ariaLabel="Sort"
                                tooltip="Sort"
                                icon={<SortIcon sx={{ fontSize: 15 }} />}
                                value={sort}
                                onChange={setSort}
                                options={[
                                    { value: "name-asc", label: "Name A → Z", group: "Name" },
                                    { value: "name-desc", label: "Name Z → A", group: "Name" },
                                    {
                                        value: "clusters-desc",
                                        label: "Most clusters",
                                        group: "Clusters",
                                    },
                                    {
                                        value: "clusters-asc",
                                        label: "Fewest clusters",
                                        group: "Clusters",
                                    },
                                    { value: "nodes-desc", label: "Most nodes", group: "Nodes" },
                                    { value: "nodes-asc", label: "Fewest nodes", group: "Nodes" },
                                ]}
                            />
                        </div>
                    </div>

                    {filtered.length > 0 ? (
                        <ul className="divide-y divide-border-subtle dark:divide-border-dark-subtle">
                            {filtered.map((ns) => (
                                <li key={ns.name}>
                                    <ResourceRow
                                        icon={<FolderIcon sx={{ fontSize: 18 }} />}
                                        title={ns.name}
                                        subtitle={
                                            ns.loading ? (
                                                <span className="inline-flex items-center gap-1.5 text-text-muted dark:text-text-dark-muted">
                                                    <span className="inline-block h-2.5 w-28 animate-pulse rounded bg-surface-muted dark:bg-surface-dark-muted" />
                                                </span>
                                            ) : (
                                                `${ns.clusterCount} clusters · ${ns.shardCount} shards · ${ns.nodeCount} nodes`
                                            )
                                        }
                                        badges={
                                            ns.loading ? null : (
                                                <>
                                                    <Chip
                                                        label={`${ns.clusterCount} clusters`}
                                                        size="small"
                                                    />
                                                    <Chip
                                                        label={`${ns.shardCount} shards`}
                                                        size="small"
                                                    />
                                                    <Chip
                                                        label={`${ns.nodeCount} nodes`}
                                                        size="small"
                                                    />
                                                </>
                                            )
                                        }
                                        href={`/namespaces/${ns.name}`}
                                        onDelete={() => handleDelete(ns.name)}
                                        deleteDisabled={deleting === ns.name}
                                    />
                                </li>
                            ))}
                        </ul>
                    ) : (
                        <div className="p-12">
                            <EmptyState
                                title={
                                    filter !== "all"
                                        ? "No matching namespaces"
                                        : "No namespaces yet"
                                }
                                description={
                                    filter !== "all"
                                        ? "Change the filter to see more results."
                                        : search
                                          ? "Try a different search term."
                                          : "Create a namespace to get started."
                                }
                                icon={<FolderOpenIcon sx={{ fontSize: 24 }} />}
                            />
                        </div>
                    )}

                    {filtered.length > 0 && (
                        <div className="border-t border-border-subtle px-6 py-3 text-xs text-text-muted dark:border-border-dark-subtle dark:text-text-dark-muted">
                            Showing {filtered.length} of {rows.length}
                            {filter !== "all" && " (filtered)"}
                        </div>
                    )}
                </div>
            </div>
        </PageShell>
    );
}
