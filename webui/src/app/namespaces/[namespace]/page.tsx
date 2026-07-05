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

import { notFound, useRouter } from "next/navigation";
import { use, useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { IconButton } from "@mui/material";
import FolderIcon from "@mui/icons-material/Folder";
import StorageIcon from "@mui/icons-material/Storage";
import ChevronRightIcon from "@mui/icons-material/ChevronRight";
import DeleteOutlineIcon from "@mui/icons-material/DeleteOutline";
import SwapHorizIcon from "@mui/icons-material/SwapHoriz";
import DownloadingIcon from "@mui/icons-material/Downloading";
import MoveToInboxIcon from "@mui/icons-material/MoveToInbox";
import AddIcon from "@mui/icons-material/Add";

import { NamespaceSidebar } from "../../ui/sidebar";
import {
    deleteCluster,
    fetchCluster,
    fetchClusters,
    fetchNamespaces,
    listNodes,
} from "@/app/lib/api";
import { LoadingSpinner } from "@/app/ui/loadingSpinner";
import EmptyState from "@/app/ui/emptyState";
import { ClusterCreation, ImportCluster } from "@/app/ui/formCreation";
import {
    FilterListIcon,
    FilterSortMenu,
    PageHeader,
    PageShell,
    SearchInput,
    SortIcon,
} from "@/app/ui/pageChrome";

const TOTAL_SLOTS = 16384;

interface ClusterData {
    name: string;
    version: string;
    shardCount: number;
    nodeCount: number;
    slotCount: number;
    slotRanges: string[];
    hasMigration: boolean;
    hasImporting: boolean;
    migratingSlot: string;
    importingSlot: string;
}

// The controller marshals `migrating_slot` / `import_slot` as `null` when the
// shard is idle and as a slot-range string (e.g. "3300" or "100-200") while
// active. We can't check with a numeric comparison because JS coerces `null`
// to `0`, which would keep "in progress" showing forever after a migration ends.
const isActiveSlot = (value: unknown): value is string =>
    typeof value === "string" && value.length > 0 && value !== "-1";

type FilterOption =
    | "all"
    | "with-migration"
    | "no-migration"
    | "with-slots"
    | "no-slots"
    | "with-importing";
type SortOption =
    | "name-asc"
    | "name-desc"
    | "shards-desc"
    | "shards-asc"
    | "nodes-desc"
    | "nodes-asc"
    | "coverage-desc"
    | "coverage-asc";

function parseSlotCount(ranges: unknown): number {
    if (!Array.isArray(ranges)) return 0;
    let total = 0;
    for (const range of ranges) {
        if (typeof range !== "string") continue;
        const parts = range.split("-");
        if (parts.length === 1) {
            const n = Number(parts[0]);
            if (!isNaN(n)) total += 1;
        } else if (parts.length === 2) {
            const a = Number(parts[0]);
            const b = Number(parts[1]);
            if (!isNaN(a) && !isNaN(b)) total += Math.max(0, b - a + 1);
        }
    }
    return total;
}

async function buildClusterData(namespace: string): Promise<{
    clusters: ClusterData[];
    totals: { shards: number; nodes: number; slots: number };
}> {
    const clusters = await fetchClusters(namespace);
    let totalShards = 0;
    let totalNodes = 0;
    let totalSlots = 0;

    const results = await Promise.all(
        clusters.map(async (cluster) => {
            try {
                const clusterInfo = await fetchCluster(namespace, cluster);
                if (!clusterInfo || typeof clusterInfo !== "object" || !("shards" in clusterInfo))
                    return null;

                const shards = ((clusterInfo as any).shards as any[]) || [];
                const nodeLists = await Promise.all(
                    shards.map((_, i) => listNodes(namespace, cluster, i.toString())),
                );
                const nodeCount = nodeLists.reduce(
                    (acc, nodes) => acc + (Array.isArray(nodes) ? nodes.length : 0),
                    0,
                );

                const slotCount = shards.reduce(
                    (acc: number, s: any) => acc + parseSlotCount(s?.slot_ranges),
                    0,
                );
                const slotRanges: string[] = shards
                    .flatMap((s: any) => (Array.isArray(s?.slot_ranges) ? s.slot_ranges : []))
                    .filter((x: unknown): x is string => typeof x === "string" && x.length > 0);

                totalShards += shards.length;
                totalNodes += nodeCount;
                totalSlots += slotCount;

                const migrating = shards.find((s: any) => isActiveSlot(s?.migrating_slot));
                const importing = shards.find((s: any) => isActiveSlot(s?.import_slot));

                return {
                    ...(clusterInfo as any),
                    shardCount: shards.length,
                    nodeCount,
                    slotCount,
                    slotRanges,
                    hasMigration: !!migrating,
                    hasImporting: !!importing,
                    migratingSlot: (migrating?.migrating_slot as string | undefined) ?? "",
                    importingSlot: (importing?.import_slot as string | undefined) ?? "",
                } as ClusterData;
            } catch (error) {
                console.error(`Failed to load cluster ${cluster}:`, error);
                return null;
            }
        }),
    );

    return {
        clusters: results.filter(Boolean) as ClusterData[],
        totals: { shards: totalShards, nodes: totalNodes, slots: totalSlots },
    };
}

function formatSummary(clusters: number, shards: number, nodes: number, coveragePct: number) {
    const parts = [
        `${clusters} ${clusters === 1 ? "cluster" : "clusters"}`,
        `${shards} ${shards === 1 ? "shard" : "shards"}`,
        `${nodes} ${nodes === 1 ? "node" : "nodes"}`,
    ];
    if (coveragePct > 0) parts.push(`${coveragePct}% slot coverage`);
    return parts.join(" · ");
}

export default function NamespacePage(props: {
    params: Promise<{ namespace: string }>;
}) {
    const params = use(props.params);
    const [rows, setRows] = useState<ClusterData[]>([]);
    const [totals, setTotals] = useState({ shards: 0, nodes: 0, slots: 0 });
    const [loading, setLoading] = useState(true);
    const [deleting, setDeleting] = useState<string | null>(null);
    const [search, setSearch] = useState("");
    const [filter, setFilter] = useState<FilterOption>("all");
    const [sort, setSort] = useState<SortOption>("name-asc");
    const router = useRouter();

    useEffect(() => {
        (async () => {
            try {
                const namespaces = await fetchNamespaces();
                if (!namespaces.includes(params.namespace)) {
                    notFound();
                    return;
                }
                const { clusters, totals: t } = await buildClusterData(params.namespace);
                setRows(clusters);
                setTotals(t);
            } catch (error) {
                console.error("Error fetching data:", error);
            } finally {
                setLoading(false);
            }
        })();
    }, [params.namespace, router]);

    const handleDelete = async (clusterName: string) => {
        if (!confirm(`Delete cluster "${clusterName}"?`)) return;
        try {
            setDeleting(clusterName);
            const res = await deleteCluster(params.namespace, clusterName);
            if (res) {
                alert(`Failed to delete cluster: ${res}`);
                return;
            }
            setLoading(true);
            const { clusters, totals: t } = await buildClusterData(params.namespace);
            setRows(clusters);
            setTotals(t);
        } catch (e) {
            alert(`Failed to delete cluster: ${e}`);
        } finally {
            setDeleting(null);
            setLoading(false);
        }
    };

    const filtered = useMemo(() => {
        return rows
            .filter((c) => {
                if (!c.name.toLowerCase().includes(search.toLowerCase())) return false;
                switch (filter) {
                    case "with-migration":
                        return c.hasMigration;
                    case "no-migration":
                        return !c.hasMigration;
                    case "with-slots":
                        return c.slotCount > 0;
                    case "no-slots":
                        return c.slotCount === 0;
                    case "with-importing":
                        return c.hasImporting;
                    default:
                        return true;
                }
            })
            .sort((a, b) => {
                switch (sort) {
                    case "name-asc":
                        return a.name.localeCompare(b.name);
                    case "name-desc":
                        return b.name.localeCompare(a.name);
                    case "shards-desc":
                        return b.shardCount - a.shardCount;
                    case "shards-asc":
                        return a.shardCount - b.shardCount;
                    case "nodes-desc":
                        return b.nodeCount - a.nodeCount;
                    case "nodes-asc":
                        return a.nodeCount - b.nodeCount;
                    case "coverage-desc":
                        return b.slotCount - a.slotCount;
                    case "coverage-asc":
                        return a.slotCount - b.slotCount;
                }
            });
    }, [rows, search, filter, sort]);

    if (loading) return <LoadingSpinner />;

    const migrating = rows.filter((r) => r.hasMigration);
    const importing = rows.filter((r) => r.hasImporting);
    const hasActivity = migrating.length > 0 || importing.length > 0;
    const coveragePct = Math.min(100, Math.round((totals.slots / TOTAL_SLOTS) * 100));
    const withSlotsCount = rows.filter((r) => r.slotCount > 0).length;

    return (
        <PageShell sidebar={<NamespaceSidebar />}>
            <PageHeader
                icon={<FolderIcon sx={{ fontSize: 20 }} />}
                title={params.namespace}
                subtitle={
                    rows.length === 0
                        ? "No clusters yet"
                        : formatSummary(rows.length, totals.shards, totals.nodes, coveragePct)
                }
                actions={
                    <SearchInput
                        value={search}
                        onChange={setSearch}
                        placeholder="Search clusters…"
                    />
                }
            />

            <div className="space-y-6 px-8 py-8 md:px-10 md:py-10">
                {hasActivity && (
                    <ActivityStrip
                        migrating={migrating}
                        importing={importing}
                        namespace={params.namespace}
                    />
                )}

                {rows.length === 0 ? (
                    <div className="pt-6">
                        <div className="mx-auto flex max-w-md flex-col items-center rounded-xl border border-dashed border-border-subtle bg-surface-subtle px-10 py-14 text-center dark:border-border-dark-subtle dark:bg-surface-dark-subtle">
                            <div className="mb-5 flex h-12 w-12 items-center justify-center rounded-xl bg-primary/10 text-primary">
                                <StorageIcon sx={{ fontSize: 24 }} />
                            </div>
                            <div className="mb-1.5 text-base font-semibold text-text-primary dark:text-text-dark-primary">
                                Add your first cluster
                            </div>
                            <p className="mb-7 max-w-xs text-sm text-text-muted dark:text-text-dark-muted">
                                A cluster is a set of shards that hold your data. Start fresh, or
                                import an existing Kvrocks deployment.
                            </p>
                            <div className="flex flex-wrap items-center justify-center gap-2">
                                <ClusterCreation
                                    position="page"
                                    namespace={params.namespace}
                                    emphasis="primary"
                                    triggerLabel="New cluster"
                                    triggerIcon={<AddIcon sx={{ fontSize: 14 }} />}
                                />
                                <ImportCluster
                                    position="page"
                                    namespace={params.namespace}
                                    emphasis="secondary"
                                    triggerLabel="Import cluster"
                                    triggerIcon={<MoveToInboxIcon sx={{ fontSize: 14 }} />}
                                />
                            </div>
                        </div>
                    </div>
                ) : (
                    <section aria-labelledby="clusters-heading">
                        <div className="mb-4 flex items-center justify-between gap-4">
                            <div className="flex items-baseline gap-2.5">
                                <h2
                                    id="clusters-heading"
                                    className="text-base font-semibold text-text-primary dark:text-text-dark-primary"
                                >
                                    Clusters
                                </h2>
                                <span className="text-sm text-text-muted dark:text-text-dark-muted">
                                    {filtered.length === rows.length
                                        ? rows.length
                                        : `${filtered.length} of ${rows.length}`}
                                </span>
                            </div>
                            <div className="flex items-center gap-1">
                                <FilterSortMenu
                                    ariaLabel="Filter clusters"
                                    tooltip="Filter"
                                    icon={<FilterListIcon sx={{ fontSize: 17 }} />}
                                    value={filter}
                                    onChange={setFilter}
                                    options={[
                                        { value: "all", label: `All (${rows.length})` },
                                        {
                                            value: "with-migration",
                                            label: `Migrating (${migrating.length})`,
                                            group: "Activity",
                                        },
                                        {
                                            value: "with-importing",
                                            label: `Importing (${importing.length})`,
                                            group: "Activity",
                                        },
                                        {
                                            value: "no-migration",
                                            label: `Stable (${rows.length - migrating.length})`,
                                            group: "Activity",
                                        },
                                        {
                                            value: "with-slots",
                                            label: `Serving slots (${withSlotsCount})`,
                                            group: "Slots",
                                        },
                                        {
                                            value: "no-slots",
                                            label: `Empty (${rows.length - withSlotsCount})`,
                                            group: "Slots",
                                        },
                                    ]}
                                />
                                <FilterSortMenu
                                    ariaLabel="Sort clusters"
                                    tooltip="Sort"
                                    icon={<SortIcon sx={{ fontSize: 17 }} />}
                                    value={sort}
                                    onChange={setSort}
                                    options={[
                                        { value: "name-asc", label: "Name A → Z", group: "Name" },
                                        { value: "name-desc", label: "Name Z → A", group: "Name" },
                                        {
                                            value: "shards-desc",
                                            label: "Most shards",
                                            group: "Shards",
                                        },
                                        {
                                            value: "shards-asc",
                                            label: "Fewest shards",
                                            group: "Shards",
                                        },
                                        {
                                            value: "nodes-desc",
                                            label: "Most nodes",
                                            group: "Nodes",
                                        },
                                        {
                                            value: "nodes-asc",
                                            label: "Fewest nodes",
                                            group: "Nodes",
                                        },
                                        {
                                            value: "coverage-desc",
                                            label: "Most slots",
                                            group: "Coverage",
                                        },
                                        {
                                            value: "coverage-asc",
                                            label: "Fewest slots",
                                            group: "Coverage",
                                        },
                                    ]}
                                />
                            </div>
                        </div>

                        {filtered.length > 0 ? (
                            <ul className="divide-y divide-border-subtle overflow-hidden rounded-xl border border-border-subtle bg-surface-base shadow-subtle dark:divide-border-dark-subtle dark:border-border-dark-subtle dark:bg-surface-dark-subtle">
                                {filtered.map((cluster) => (
                                    <li key={cluster.name}>
                                        <ClusterRow
                                            cluster={cluster}
                                            namespace={params.namespace}
                                            onDelete={handleDelete}
                                            deleting={deleting === cluster.name}
                                        />
                                    </li>
                                ))}
                            </ul>
                        ) : (
                            <div className="py-8">
                                <EmptyState
                                    title="No matching clusters"
                                    description={
                                        search
                                            ? "Try a different search term."
                                            : "Change the filter to see more results."
                                    }
                                    icon={<StorageIcon sx={{ fontSize: 24 }} />}
                                />
                            </div>
                        )}
                    </section>
                )}
            </div>
        </PageShell>
    );
}

function ActivityStrip({
    migrating,
    importing,
    namespace,
}: {
    migrating: ClusterData[];
    importing: ClusterData[];
    namespace: string;
}) {
    const bits: string[] = [];
    if (migrating.length > 0)
        bits.push(
            `${migrating.length} ${migrating.length === 1 ? "cluster" : "clusters"} migrating`,
        );
    if (importing.length > 0)
        bits.push(
            `${importing.length} ${importing.length === 1 ? "cluster" : "clusters"} importing`,
        );

    const active = [
        ...migrating,
        ...importing.filter((c) => !migrating.some((m) => m.name === c.name)),
    ].slice(0, 5);

    return (
        <div className="flex flex-col gap-3 rounded-xl border border-warning/30 bg-warning/5 px-5 py-4 dark:border-warning/25 dark:bg-warning/10 sm:flex-row sm:items-center sm:justify-between">
            <div className="flex items-center gap-3.5">
                <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-lg bg-warning/15 text-warning">
                    <SwapHorizIcon sx={{ fontSize: 18 }} />
                </div>
                <div>
                    <div className="text-sm font-semibold text-text-primary dark:text-text-dark-primary">
                        Slot movement in progress
                    </div>
                    <div className="mt-0.5 text-sm text-text-muted dark:text-text-dark-muted">
                        {bits.join(" · ")}
                    </div>
                </div>
            </div>
            <div className="flex flex-wrap items-center gap-1.5">
                {active.map((c) => (
                    <Link
                        key={c.name}
                        href={`/namespaces/${namespace}/clusters/${c.name}`}
                        className="inline-flex items-center gap-1 rounded-md border border-border-subtle bg-surface-base px-2 py-0.5 text-2xs font-medium text-text-secondary transition-colors hover:border-border-strong hover:text-text-primary dark:border-border-dark-subtle dark:bg-surface-dark-base dark:text-text-dark-secondary dark:hover:border-border-dark-strong dark:hover:text-text-dark-primary"
                    >
                        <span
                            className={`h-1 w-1 shrink-0 rounded-full ${
                                c.hasMigration ? "bg-warning" : "bg-info"
                            } animate-pulse`}
                            aria-hidden
                        />
                        {c.name}
                    </Link>
                ))}
            </div>
        </div>
    );
}

function ClusterRow({
    cluster,
    namespace,
    onDelete,
    deleting,
}: {
    cluster: ClusterData;
    namespace: string;
    onDelete: (name: string) => void;
    deleting: boolean;
}) {
    const coverage = Math.min(100, Math.round((cluster.slotCount / TOTAL_SLOTS) * 100));
    const state: "migrating" | "importing" | "empty" | "stable" = cluster.hasMigration
        ? "migrating"
        : cluster.hasImporting
          ? "importing"
          : cluster.slotCount === 0
            ? "empty"
            : "stable";

    const dotClass =
        state === "migrating"
            ? "bg-warning animate-pulse"
            : state === "importing"
              ? "bg-info animate-pulse"
              : state === "empty"
                ? "bg-text-muted/40 dark:bg-text-dark-muted/40"
                : "bg-success";

    const barClass =
        state === "empty"
            ? "bg-text-muted/25 dark:bg-text-dark-muted/25"
            : state === "migrating"
              ? "bg-warning/80"
              : state === "importing"
                ? "bg-info/80"
                : "bg-primary/80";

    const stateLabel =
        state === "migrating"
            ? "Migrating"
            : state === "importing"
              ? "Importing"
              : state === "empty"
                ? "No slots"
                : "Stable";

    return (
        <Link
            href={`/namespaces/${namespace}/clusters/${cluster.name}`}
            className="group grid grid-cols-[auto_minmax(0,1fr)_auto] items-center gap-4 px-6 py-4 transition-colors hover:bg-surface-hover dark:hover:bg-surface-dark-hover md:grid-cols-[auto_minmax(0,1fr)_auto_auto]"
            aria-label={`${cluster.name}, ${stateLabel.toLowerCase()}`}
        >
            <span
                className={`h-2 w-2 shrink-0 rounded-full ${dotClass}`}
                aria-hidden
            />

            <div className="min-w-0">
                <div className="flex items-center gap-2">
                    <span className="truncate text-sm font-semibold text-text-primary dark:text-text-dark-primary">
                        {cluster.name}
                    </span>
                    {cluster.version && (
                        <span className="rounded bg-surface-muted px-1 py-0.5 font-mono text-2xs text-text-muted dark:bg-surface-dark-muted dark:text-text-dark-muted">
                            v{cluster.version}
                        </span>
                    )}
                    {cluster.hasMigration && (
                        <span className="inline-flex items-center gap-1 rounded-md border border-warning/40 bg-warning/10 px-1.5 py-0.5 text-2xs font-medium text-warning">
                            <SwapHorizIcon sx={{ fontSize: 11 }} />
                            slot {cluster.migratingSlot}
                        </span>
                    )}
                    {cluster.hasImporting && (
                        <span className="inline-flex items-center gap-1 rounded-md border border-info/40 bg-info/10 px-1.5 py-0.5 text-2xs font-medium text-info">
                            <DownloadingIcon sx={{ fontSize: 11 }} />
                            slot {cluster.importingSlot}
                        </span>
                    )}
                </div>
                <div className="mt-0.5 flex flex-wrap items-center gap-x-2 gap-y-0.5 text-xs text-text-muted dark:text-text-dark-muted">
                    <span>
                        <span className="font-medium text-text-secondary dark:text-text-dark-secondary">
                            {cluster.shardCount}
                        </span>{" "}
                        {cluster.shardCount === 1 ? "shard" : "shards"}
                    </span>
                    <span className="opacity-40" aria-hidden>
                        ·
                    </span>
                    <span>
                        <span className="font-medium text-text-secondary dark:text-text-dark-secondary">
                            {cluster.nodeCount}
                        </span>{" "}
                        {cluster.nodeCount === 1 ? "node" : "nodes"}
                    </span>
                    <span className="opacity-40" aria-hidden>
                        ·
                    </span>
                    <span>
                        <span className="font-medium text-text-secondary dark:text-text-dark-secondary">
                            {cluster.slotCount.toLocaleString()}
                        </span>{" "}
                        {cluster.slotCount === 1 ? "slot" : "slots"}
                    </span>
                </div>
            </div>

            <div className="hidden w-32 shrink-0 md:block" aria-hidden>
                <div
                    className="h-1 w-full overflow-hidden rounded-full bg-surface-muted dark:bg-surface-dark-muted"
                    title={`${coverage}% slot coverage`}
                >
                    <div
                        className={`h-full rounded-full transition-[width] duration-500 ease-out ${barClass}`}
                        style={{ width: `${coverage}%` }}
                    />
                </div>
                <div className="mt-1 flex items-center justify-between text-2xs text-text-muted dark:text-text-dark-muted">
                    <span>{stateLabel}</span>
                    <span className="font-medium text-text-secondary tabular-nums dark:text-text-dark-secondary">
                        {coverage}%
                    </span>
                </div>
            </div>

            <div className="flex shrink-0 items-center gap-0.5">
                <IconButton
                    size="small"
                    onClick={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                        onDelete(cluster.name);
                    }}
                    disabled={deleting}
                    aria-label={`Delete cluster ${cluster.name}`}
                    sx={{
                        width: 26,
                        height: 26,
                        opacity: 0.6,
                        "&:hover": { color: "error.main", opacity: 1 },
                    }}
                >
                    <DeleteOutlineIcon sx={{ fontSize: 15 }} />
                </IconButton>
                <ChevronRightIcon
                    sx={{ fontSize: 15 }}
                    className="text-text-muted transition-transform group-hover:translate-x-0.5 dark:text-text-dark-muted"
                />
            </div>
        </Link>
    );
}
