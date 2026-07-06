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
import Link from "next/link";
import { Button, Chip } from "@mui/material";
import DnsIcon from "@mui/icons-material/Dns";
import StorageIcon from "@mui/icons-material/Storage";
import DeviceHubIcon from "@mui/icons-material/DeviceHub";
import GridViewIcon from "@mui/icons-material/GridView";
import SwapHorizIcon from "@mui/icons-material/SwapHoriz";
import MoveUpIcon from "@mui/icons-material/MoveUp";

import { ClusterSidebar } from "../../../../ui/sidebar";
import { deleteShard, listShards } from "@/app/lib/api";
import { LoadingSpinner } from "@/app/ui/loadingSpinner";
import EmptyState from "@/app/ui/emptyState";
import { ShardCreation } from "@/app/ui/formCreation";
import { MigrationDialog } from "@/app/ui/migrationDialog";
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

interface ShardData {
    index: number;
    nodes: any[];
    slotRanges: string[];
    migratingSlot: string;
    importingSlot: string;
    targetShardIndex: number;
    nodeCount: number;
    hasSlots: boolean;
    hasMigration: boolean;
    hasImporting: boolean;
}

type FilterOption =
    "all" | "with-migration" | "no-migration" | "with-slots" | "no-slots" | "with-importing";
type SortOption = "index-asc" | "index-desc" | "nodes-desc" | "nodes-asc";

const isActive = (value: string | null | undefined) =>
    value !== null && value !== undefined && value !== "" && value !== "-1";

function summarise(shards: any[]): {
    data: ShardData[];
    nodes: number;
    withSlots: number;
    migrating: number;
} {
    let nodes = 0;
    let withSlots = 0;
    let migrating = 0;

    const data = shards.map((shard: any, index: number) => {
        const nodeCount = shard.nodes?.length || 0;
        nodes += nodeCount;
        const hasSlots = shard.slot_ranges && shard.slot_ranges.length > 0;
        if (hasSlots) withSlots++;
        const migratingSlot = shard.migrating_slot || "";
        const importingSlot = shard.import_slot || "";
        const hasMigration = isActive(migratingSlot);
        if (hasMigration) migrating++;
        return {
            index,
            nodes: shard.nodes || [],
            slotRanges: shard.slot_ranges || [],
            migratingSlot,
            importingSlot,
            targetShardIndex: shard.target_shard_index ?? -1,
            nodeCount,
            hasSlots,
            hasMigration,
            hasImporting: isActive(importingSlot),
        };
    });
    return { data, nodes, withSlots, migrating };
}

export default function ClusterPage(props: {
    params: Promise<{ namespace: string; cluster: string }>;
}) {
    const params = use(props.params);
    const { namespace, cluster } = params;
    const [rows, setRows] = useState<ShardData[]>([]);
    const [totals, setTotals] = useState({ shards: 0, nodes: 0, withSlots: 0, migrating: 0 });
    const [loading, setLoading] = useState(true);
    const [deleting, setDeleting] = useState<number | null>(null);
    const [search, setSearch] = useState("");
    const [filter, setFilter] = useState<FilterOption>("all");
    const [sort, setSort] = useState<SortOption>("index-asc");
    const [migrationOpen, setMigrationOpen] = useState(false);
    const router = useRouter();

    const refresh = useCallback(async () => {
        try {
            const shards = await listShards(namespace, cluster);
            if (!shards) {
                router.push("/404");
                return;
            }
            const { data, nodes, withSlots, migrating } = summarise(shards as any[]);
            setRows(data);
            setTotals({ shards: data.length, nodes, withSlots, migrating });
        } catch (error) {
            console.error("Error fetching shards:", error);
        }
    }, [namespace, cluster, router]);

    useEffect(() => {
        (async () => {
            setLoading(true);
            await refresh();
            setLoading(false);
        })();
    }, [refresh]);

    const handleDelete = async (index: number) => {
        if (!confirm(`Delete Shard ${index + 1}? This cannot be undone.`)) return;
        try {
            setDeleting(index);
            const res = await deleteShard(namespace, cluster, index.toString());
            if (res) {
                alert(`Failed to delete shard: ${res}`);
                return;
            }
            await refresh();
        } catch (e) {
            alert(`Failed to delete shard: ${e}`);
        } finally {
            setDeleting(null);
        }
    };

    const filtered = useMemo(() => {
        return rows
            .filter((s) => {
                if (!`shard ${s.index + 1}`.toLowerCase().includes(search.toLowerCase()))
                    return false;
                switch (filter) {
                    case "with-migration":
                        return s.hasMigration;
                    case "no-migration":
                        return !s.hasMigration;
                    case "with-slots":
                        return s.hasSlots;
                    case "no-slots":
                        return !s.hasSlots;
                    case "with-importing":
                        return s.hasImporting;
                    default:
                        return true;
                }
            })
            .sort((a, b) => {
                switch (sort) {
                    case "index-asc":
                        return a.index - b.index;
                    case "index-desc":
                        return b.index - a.index;
                    case "nodes-desc":
                        return b.nodeCount - a.nodeCount;
                    case "nodes-asc":
                        return a.nodeCount - b.nodeCount;
                }
            });
    }, [rows, search, filter, sort]);

    if (loading) return <LoadingSpinner />;

    const migrationShards = rows.map((r) => ({
        ...r,
        migratingSlot: String(r.migratingSlot),
        importingSlot: String(r.importingSlot),
    }));

    return (
        <PageShell sidebar={<ClusterSidebar namespace={namespace} />}>
            <PageHeader
                icon={<StorageIcon sx={{ fontSize: 16 }} />}
                title={cluster}
                subtitle={`Cluster in ${namespace}`}
                actions={
                    <>
                        <SearchInput
                            value={search}
                            onChange={setSearch}
                            placeholder="Search shards…"
                        />
                        <ShardCreation position="page" namespace={namespace} cluster={cluster} />
                        <Button
                            variant="outlined"
                            size="small"
                            startIcon={<MoveUpIcon sx={{ fontSize: 13 }} />}
                            onClick={() => setMigrationOpen(true)}
                            disabled={totals.withSlots === 0}
                        >
                            Migrate slot
                        </Button>
                    </>
                }
            />

            <div className="px-8 py-8 md:px-10 md:py-10">
                <div className="mb-8 grid grid-cols-2 gap-4 sm:grid-cols-4">
                    <StatCard
                        label="Shards"
                        value={totals.shards}
                        icon={<DnsIcon sx={{ fontSize: 20 }} />}
                        accent="primary"
                    />
                    <StatCard
                        label="Nodes"
                        value={totals.nodes}
                        icon={<DeviceHubIcon sx={{ fontSize: 20 }} />}
                        accent="warning"
                    />
                    <StatCard
                        label="With slots"
                        value={totals.withSlots}
                        icon={<GridViewIcon sx={{ fontSize: 20 }} />}
                        accent="info"
                    />
                    <StatCard
                        label="Migrating"
                        value={totals.migrating}
                        icon={<SwapHorizIcon sx={{ fontSize: 20 }} />}
                        accent={totals.migrating > 0 ? "warning" : "default"}
                    />
                </div>

                <div className="overflow-hidden rounded-xl border border-border-subtle bg-surface-base shadow-subtle dark:border-border-dark-subtle dark:bg-surface-dark-subtle">
                    <div className="flex items-center justify-between border-b border-border-subtle px-6 py-3.5 dark:border-border-dark-subtle">
                        <div className="lin-eyebrow">All shards</div>
                        <div className="flex items-center gap-1">
                            <FilterSortMenu
                                ariaLabel="Filter"
                                tooltip="Filter"
                                icon={<FilterListIcon sx={{ fontSize: 17 }} />}
                                value={filter}
                                onChange={setFilter}
                                options={[
                                    { value: "all", label: `All (${rows.length})` },
                                    {
                                        value: "with-migration",
                                        label: `Migrating (${rows.filter((r) => r.hasMigration).length})`,
                                    },
                                    {
                                        value: "no-migration",
                                        label: `Stable (${rows.filter((r) => !r.hasMigration).length})`,
                                    },
                                    {
                                        value: "with-slots",
                                        label: `With slots (${rows.filter((r) => r.hasSlots).length})`,
                                    },
                                    {
                                        value: "no-slots",
                                        label: `Without slots (${rows.filter((r) => !r.hasSlots).length})`,
                                    },
                                    {
                                        value: "with-importing",
                                        label: `Importing (${rows.filter((r) => r.hasImporting).length})`,
                                    },
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
                                    { value: "nodes-desc", label: "Most nodes", group: "Nodes" },
                                    { value: "nodes-asc", label: "Fewest nodes", group: "Nodes" },
                                ]}
                            />
                        </div>
                    </div>

                    {filtered.length > 0 ? (
                        <ul className="divide-y divide-border-subtle dark:divide-border-dark-subtle">
                            {filtered.map((shard) => {
                                const slotSummary = shard.slotRanges.length
                                    ? shard.slotRanges.length > 2
                                        ? `${shard.slotRanges.slice(0, 2).join(", ")} (+${shard.slotRanges.length - 2})`
                                        : shard.slotRanges.join(", ")
                                    : "No slots";
                                return (
                                    <li key={shard.index}>
                                        <ResourceRow
                                            icon={<DnsIcon sx={{ fontSize: 18 }} />}
                                            title={`Shard ${shard.index + 1}`}
                                            subtitle={`${shard.nodeCount} nodes · slots: ${slotSummary}`}
                                            badges={
                                                <>
                                                    {shard.hasMigration && (
                                                        <span className="flex items-center gap-1 rounded-md border border-warning/40 bg-warning/10 px-1.5 py-0.5 text-2xs font-medium text-warning">
                                                            <span className="h-1 w-1 animate-pulse rounded-full bg-warning" />
                                                            Migrating {shard.migratingSlot}
                                                        </span>
                                                    )}
                                                    {shard.hasImporting && (
                                                        <span className="flex items-center gap-1 rounded-md border border-info/40 bg-info/10 px-1.5 py-0.5 text-2xs font-medium text-info">
                                                            <span className="h-1 w-1 animate-pulse rounded-full bg-info" />
                                                            Importing {shard.importingSlot}
                                                        </span>
                                                    )}
                                                    <Chip
                                                        label={`${shard.nodeCount} nodes`}
                                                        size="small"
                                                    />
                                                </>
                                            }
                                            href={`/namespaces/${namespace}/clusters/${cluster}/shards/${shard.index}`}
                                            onDelete={() => handleDelete(shard.index)}
                                            deleteDisabled={deleting === shard.index}
                                        />
                                    </li>
                                );
                            })}
                        </ul>
                    ) : (
                        <div className="p-12">
                            <EmptyState
                                title={filter !== "all" ? "No matching shards" : "No shards yet"}
                                description={
                                    filter !== "all"
                                        ? "Change the filter to see more results."
                                        : search
                                          ? "Try a different search term."
                                          : "Create a shard to get started."
                                }
                                icon={<DnsIcon sx={{ fontSize: 24 }} />}
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

            <MigrationDialog
                open={migrationOpen}
                onClose={() => setMigrationOpen(false)}
                namespace={namespace}
                cluster={cluster}
                shards={migrationShards}
                onSuccess={refresh}
            />
        </PageShell>
    );
}
