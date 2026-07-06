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

import { ReactNode, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import SearchIcon from "@mui/icons-material/Search";
import CloseIcon from "@mui/icons-material/Close";
import FolderOpenIcon from "@mui/icons-material/FolderOpen";
import StorageIcon from "@mui/icons-material/Storage";
import DnsIcon from "@mui/icons-material/Dns";
import DeviceHubIcon from "@mui/icons-material/DeviceHub";
import Item from "./sidebarItem";
import { ClusterCreation, NamespaceCreation, NodeCreation, ShardCreation } from "./formCreation";
import { fetchClusters, fetchNamespaces, listNodes, listShards } from "@/app/lib/api";

const FILTER_THRESHOLD = 5;
const SKELETON_WIDTHS = ["68%", "52%", "78%", "58%", "44%"];

interface SidebarShellProps {
    label: string;
    total: number;
    filteredCount: number;
    error: string | null;
    loading: boolean;
    action: ReactNode;
    filterValue: string;
    onFilterChange: (v: string) => void;
    filterPlaceholder: string;
    emptyIcon: ReactNode;
    emptyTitle: string;
    emptyHint: string;
    children: ReactNode;
}

function SidebarShell({
    label,
    total,
    filteredCount,
    error,
    loading,
    action,
    filterValue,
    onFilterChange,
    filterPlaceholder,
    emptyIcon,
    emptyTitle,
    emptyHint,
    children,
}: SidebarShellProps) {
    const isEmpty = !loading && total === 0;
    const isFilteredEmpty = !loading && total > 0 && filteredCount === 0;
    const showFilter = !loading && total > FILTER_THRESHOLD;
    const isFiltering = filterValue.trim().length > 0;

    return (
        <aside className="hidden w-[var(--lin-sidebar-width)] shrink-0 border-r border-border-subtle bg-surface-subtle dark:border-border-dark-subtle dark:bg-surface-dark-subtle md:block">
            <div className="sticky top-[calc(var(--lin-topbar-height)+44px)] flex h-[calc(100vh-var(--lin-topbar-height)-44px)] flex-col">
                <div className="flex items-center justify-between gap-2 px-5 pb-3 pt-5">
                    <div className="flex min-w-0 items-center gap-2.5">
                        <span className="lin-eyebrow">{label}</span>
                        <span className="rounded-md bg-surface-muted px-2 py-0.5 text-2xs font-medium tabular-nums text-text-muted dark:bg-surface-dark-muted dark:text-text-dark-muted">
                            {isFiltering && !loading && filteredCount !== total
                                ? `${filteredCount}/${total}`
                                : total}
                        </span>
                    </div>
                </div>

                <div className="px-4 pb-2">{action}</div>

                {showFilter && (
                    <div className="relative mx-4 mb-2">
                        <SearchIcon
                            sx={{ fontSize: 14 }}
                            className="pointer-events-none absolute left-2.5 top-1/2 -translate-y-1/2 text-text-muted dark:text-text-dark-muted"
                        />
                        <input
                            type="text"
                            value={filterValue}
                            onChange={(e) => onFilterChange(e.target.value)}
                            placeholder={filterPlaceholder}
                            aria-label={`Filter ${label.toLowerCase()}`}
                            className="h-8 w-full rounded-md border border-border-subtle bg-surface-base pl-8 pr-7 text-xs text-text-primary transition-colors placeholder:text-text-muted hover:border-border-strong focus:border-primary focus:outline-none focus:ring-2 focus:ring-primary/20 dark:border-border-dark-subtle dark:bg-surface-dark-base dark:text-text-dark-primary dark:placeholder:text-text-dark-muted"
                        />
                        {filterValue && (
                            <button
                                type="button"
                                onClick={() => onFilterChange("")}
                                aria-label="Clear filter"
                                className="absolute right-1 top-1/2 -translate-y-1/2 rounded p-1 text-text-muted hover:bg-surface-hover hover:text-text-primary dark:text-text-dark-muted dark:hover:bg-surface-dark-hover dark:hover:text-text-dark-primary"
                            >
                                <CloseIcon sx={{ fontSize: 12 }} />
                            </button>
                        )}
                    </div>
                )}

                {error && (
                    <div className="mx-4 mb-3 rounded-md border border-error/30 bg-error/5 px-2.5 py-2 text-xs text-error dark:border-error/40">
                        {error}
                    </div>
                )}

                <div className="min-h-0 flex-1 overflow-y-auto px-3 pb-5">
                    {loading ? (
                        <SidebarSkeleton />
                    ) : isEmpty ? (
                        <SidebarEmpty icon={emptyIcon} title={emptyTitle} hint={emptyHint} />
                    ) : isFilteredEmpty ? (
                        <SidebarNoMatches query={filterValue} onClear={() => onFilterChange("")} />
                    ) : (
                        children
                    )}
                </div>
            </div>
        </aside>
    );
}

function SidebarSkeleton() {
    return (
        <ul className="space-y-1" aria-hidden>
            {SKELETON_WIDTHS.map((w, i) => (
                <li key={i} className="flex h-9 items-center gap-2.5 px-3">
                    <span className="h-3.5 w-3.5 shrink-0 animate-pulse rounded bg-surface-muted dark:bg-surface-dark-muted" />
                    <span
                        className="h-3 animate-pulse rounded bg-surface-muted dark:bg-surface-dark-muted"
                        style={{ width: w }}
                    />
                </li>
            ))}
        </ul>
    );
}

function SidebarEmpty({ icon, title, hint }: { icon: ReactNode; title: string; hint: string }) {
    return (
        <div className="mx-1 mt-2 flex flex-col items-center rounded-lg border border-dashed border-border-subtle bg-surface-base/60 px-4 py-6 text-center dark:border-border-dark-subtle dark:bg-surface-dark-base/40">
            <div className="mb-2 flex h-8 w-8 items-center justify-center rounded-md bg-surface-muted text-text-muted dark:bg-surface-dark-muted dark:text-text-dark-muted">
                {icon}
            </div>
            <div className="text-xs font-medium text-text-primary dark:text-text-dark-primary">
                {title}
            </div>
            <p className="mt-1 text-2xs leading-relaxed text-text-muted dark:text-text-dark-muted">
                {hint}
            </p>
        </div>
    );
}

function SidebarNoMatches({ query, onClear }: { query: string; onClear: () => void }) {
    return (
        <div className="mx-1 mt-2 rounded-lg border border-dashed border-border-subtle bg-surface-base/60 px-4 py-4 text-center dark:border-border-dark-subtle dark:bg-surface-dark-base/40">
            <div className="text-xs text-text-secondary dark:text-text-dark-secondary">
                No matches for{" "}
                <span className="font-medium text-text-primary dark:text-text-dark-primary">
                    &ldquo;{query}&rdquo;
                </span>
            </div>
            <button
                type="button"
                onClick={onClear}
                className="mt-2 text-2xs font-medium text-primary hover:underline"
            >
                Clear filter
            </button>
        </div>
    );
}

const matchesQuery = (haystack: string, query: string) => {
    if (!query) return true;
    return haystack.replace(/\t/g, " ").toLowerCase().includes(query.toLowerCase());
};

// Scrolls the currently-active list item into view when the sidebar mounts or
// the pathname changes. Uses layout effect so users deep-linking to a node
// don't see a jump after paint.
function useScrollActiveIntoView(deps: unknown[]) {
    const ref = useRef<HTMLUListElement | null>(null);
    useLayoutEffect(() => {
        if (!ref.current) return;
        const active = ref.current.querySelector<HTMLElement>('[data-active="true"]');
        if (active) active.scrollIntoView({ block: "nearest" });
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, deps);
    return ref;
}

export function NamespaceSidebar() {
    const [namespaces, setNamespaces] = useState<string[]>([]);
    const [error, setError] = useState<string | null>(null);
    const [loading, setLoading] = useState(true);
    const [filter, setFilter] = useState("");
    const pathname = usePathname();
    const activeSlug = pathname.startsWith("/namespaces/") ? pathname.split("/")[2] : null;

    useEffect(() => {
        (async () => {
            try {
                setNamespaces(await fetchNamespaces());
            } catch {
                setError("Failed to fetch namespaces");
            } finally {
                setLoading(false);
            }
        })();
    }, []);

    const filtered = useMemo(
        () => namespaces.filter((n) => matchesQuery(n, filter)),
        [namespaces, filter]
    );
    const listRef = useScrollActiveIntoView([loading, activeSlug, filtered.length]);

    return (
        <SidebarShell
            label="Namespaces"
            total={namespaces.length}
            filteredCount={filtered.length}
            error={error}
            loading={loading}
            action={
                <NamespaceCreation
                    position="sidebar"
                    emphasis="primary"
                    triggerLabel="New namespace"
                />
            }
            filterValue={filter}
            onFilterChange={setFilter}
            filterPlaceholder="Filter namespaces…"
            emptyIcon={<FolderOpenIcon sx={{ fontSize: 16 }} />}
            emptyTitle="No namespaces"
            emptyHint="Create a namespace to organize your Kvrocks clusters."
        >
            <ul ref={listRef} className="space-y-1">
                {filtered.map((namespace) => (
                    <li key={namespace} data-active={namespace === activeSlug}>
                        <Link href={`/namespaces/${namespace}`} passHref>
                            <Item type="namespace" item={namespace} />
                        </Link>
                    </li>
                ))}
            </ul>
        </SidebarShell>
    );
}

export function ClusterSidebar({ namespace }: { namespace: string }) {
    const [clusters, setClusters] = useState<string[]>([]);
    const [error, setError] = useState<string | null>(null);
    const [loading, setLoading] = useState(true);
    const [filter, setFilter] = useState("");
    const pathname = usePathname();
    const parts = pathname.split("/");
    const activeSlug = parts[3] === "clusters" ? (parts[4] ?? null) : null;

    useEffect(() => {
        setLoading(true);
        setFilter("");
        (async () => {
            try {
                setClusters(await fetchClusters(namespace));
            } catch {
                setError("Failed to fetch clusters");
            } finally {
                setLoading(false);
            }
        })();
    }, [namespace]);

    const filtered = useMemo(
        () => clusters.filter((c) => matchesQuery(c, filter)),
        [clusters, filter]
    );
    const listRef = useScrollActiveIntoView([loading, activeSlug, filtered.length]);

    return (
        <SidebarShell
            label="Clusters"
            total={clusters.length}
            filteredCount={filtered.length}
            error={error}
            loading={loading}
            action={<ClusterCreation position="sidebar" namespace={namespace} />}
            filterValue={filter}
            onFilterChange={setFilter}
            filterPlaceholder="Filter clusters…"
            emptyIcon={<StorageIcon sx={{ fontSize: 16 }} />}
            emptyTitle="No clusters"
            emptyHint={`Add a cluster to ${namespace} to start managing shards and nodes.`}
        >
            <ul ref={listRef} className="space-y-1">
                {filtered.map((cluster) => (
                    <li key={cluster} data-active={cluster === activeSlug}>
                        <Link href={`/namespaces/${namespace}/clusters/${cluster}`} passHref>
                            <Item type="cluster" item={cluster} namespace={namespace} />
                        </Link>
                    </li>
                ))}
            </ul>
        </SidebarShell>
    );
}

export function ShardSidebar({ namespace, cluster }: { namespace: string; cluster: string }) {
    const [shardCount, setShardCount] = useState(0);
    const [error, setError] = useState<string | null>(null);
    const [loading, setLoading] = useState(true);
    const [filter, setFilter] = useState("");
    const pathname = usePathname();
    const parts = pathname.split("/");
    const activeSlug = parts[5] === "shards" ? (parts[6] ?? null) : null;

    useEffect(() => {
        setLoading(true);
        setFilter("");
        (async () => {
            try {
                const list = await listShards(namespace, cluster);
                setShardCount(Array.isArray(list) ? list.length : 0);
            } catch {
                setError("Failed to fetch shards");
            } finally {
                setLoading(false);
            }
        })();
    }, [namespace, cluster]);

    const shards = useMemo(
        () => Array.from({ length: shardCount }, (_, i) => `Shard\t${i + 1}`),
        [shardCount]
    );
    const filtered = useMemo(() => shards.filter((s) => matchesQuery(s, filter)), [shards, filter]);
    const listRef = useScrollActiveIntoView([loading, activeSlug, filtered.length]);

    return (
        <SidebarShell
            label="Shards"
            total={shards.length}
            filteredCount={filtered.length}
            error={error}
            loading={loading}
            action={<ShardCreation position="sidebar" namespace={namespace} cluster={cluster} />}
            filterValue={filter}
            onFilterChange={setFilter}
            filterPlaceholder="Filter shards…"
            emptyIcon={<DnsIcon sx={{ fontSize: 16 }} />}
            emptyTitle="No shards"
            emptyHint={`Create a shard in ${cluster} to distribute your data.`}
        >
            <ul ref={listRef} className="space-y-1">
                {filtered.map((shard) => {
                    const index = parseInt(shard.split("\t")[1]) - 1;
                    return (
                        <li key={index} data-active={String(index) === activeSlug}>
                            <Link
                                href={`/namespaces/${namespace}/clusters/${cluster}/shards/${index}`}
                                passHref
                            >
                                <Item
                                    type="shard"
                                    item={shard}
                                    namespace={namespace}
                                    cluster={cluster}
                                />
                            </Link>
                        </li>
                    );
                })}
            </ul>
        </SidebarShell>
    );
}

interface NodeItem {
    addr: string;
    created_at: number;
    id: string;
    password: string;
    role: string;
}

export function NodeSidebar({
    namespace,
    cluster,
    shard,
}: {
    namespace: string;
    cluster: string;
    shard: string;
}) {
    const [nodes, setNodes] = useState<NodeItem[]>([]);
    const [error, setError] = useState<string | null>(null);
    const [loading, setLoading] = useState(true);
    const [filter, setFilter] = useState("");
    const pathname = usePathname();
    const parts = pathname.split("/");
    const activeSlug = parts[7] === "nodes" ? (parts[8] ?? null) : null;

    useEffect(() => {
        setLoading(true);
        setFilter("");
        (async () => {
            try {
                const fetched = (await listNodes(namespace, cluster, shard)) as NodeItem[];
                setNodes(fetched);
            } catch {
                setError("Failed to fetch nodes");
            } finally {
                setLoading(false);
            }
        })();
    }, [namespace, cluster, shard]);

    const filtered = useMemo(() => {
        return nodes
            .map((node, index) => ({ node, index }))
            .filter(({ node, index }) => {
                const label = `Node ${index + 1}`;
                return (
                    matchesQuery(label, filter) ||
                    matchesQuery(node.addr ?? "", filter) ||
                    matchesQuery(node.id ?? "", filter)
                );
            });
    }, [nodes, filter]);
    const listRef = useScrollActiveIntoView([loading, activeSlug, filtered.length]);

    return (
        <SidebarShell
            label="Nodes"
            total={nodes.length}
            filteredCount={filtered.length}
            error={error}
            loading={loading}
            action={
                <NodeCreation
                    position="sidebar"
                    namespace={namespace}
                    cluster={cluster}
                    shard={shard}
                />
            }
            filterValue={filter}
            onFilterChange={setFilter}
            filterPlaceholder="Filter nodes, addr, or id…"
            emptyIcon={<DeviceHubIcon sx={{ fontSize: 16 }} />}
            emptyTitle="No nodes"
            emptyHint="Add a node to bring this shard online."
        >
            <ul ref={listRef} className="space-y-1">
                {filtered.map(({ node, index }) => (
                    <li key={index} data-active={String(index) === activeSlug}>
                        <Link
                            href={`/namespaces/${namespace}/clusters/${cluster}/shards/${shard}/nodes/${index}`}
                            passHref
                        >
                            <Item
                                type="node"
                                item={`Node\t${index + 1}`}
                                id={node.id}
                                namespace={namespace}
                                cluster={cluster}
                                shard={shard}
                            />
                        </Link>
                    </li>
                ))}
            </ul>
        </SidebarShell>
    );
}
