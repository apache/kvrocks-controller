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

import {
    Box,
    Typography,
    Chip,
    Paper,
    Grid,
    Button,
    IconButton,
    Tooltip,
    Popover,
    RadioGroup,
    FormControlLabel,
    Radio,
    Fade,
    Badge,
    Collapse,
    Divider,
} from "@mui/material";
import { ClusterSidebar } from "../../../../ui/sidebar";
import { useState, useEffect } from "react";
import { listShards, listNodes, fetchCluster, deleteShard } from "@/app/lib/api";
import { AddShardCard, ResourceCard } from "@/app/ui/createCard";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { LoadingSpinner } from "@/app/ui/loadingSpinner";
import DnsIcon from "@mui/icons-material/Dns";
import StorageIcon from "@mui/icons-material/Storage";
import DeviceHubIcon from "@mui/icons-material/DeviceHub";
import EmptyState from "@/app/ui/emptyState";
import SearchIcon from "@mui/icons-material/Search";
import FilterListIcon from "@mui/icons-material/FilterList";
import SortIcon from "@mui/icons-material/Sort";
import CheckIcon from "@mui/icons-material/Check";
import ArrowUpwardIcon from "@mui/icons-material/ArrowUpward";
import ArrowDownwardIcon from "@mui/icons-material/ArrowDownward";
import ChevronRightIcon from "@mui/icons-material/ChevronRight";
import AddIcon from "@mui/icons-material/Add";
import WarningIcon from "@mui/icons-material/Warning";
import InfoIcon from "@mui/icons-material/Info";
import SwapHorizIcon from "@mui/icons-material/SwapHoriz";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import ExpandLessIcon from "@mui/icons-material/ExpandLess";
import { ShardCreation } from "@/app/ui/formCreation";
import DeleteIcon from "@mui/icons-material/Delete";
import MoveUpIcon from "@mui/icons-material/MoveUp";
import { MigrationDialog } from "@/app/ui/migrationDialog";

interface ResourceCounts {
    shards: number;
    nodes: number;
    withSlots: number;
    migrating: number;
}

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
    | "all"
    | "with-migration"
    | "no-migration"
    | "with-slots"
    | "no-slots"
    | "with-importing";
type SortOption = "index-asc" | "index-desc" | "nodes-desc" | "nodes-asc";

export default function Cluster({ params }: { params: { namespace: string; cluster: string } }) {
    const { namespace, cluster } = params;
    const [shardsData, setShardsData] = useState<ShardData[]>([]);
    const [resourceCounts, setResourceCounts] = useState<ResourceCounts>({
        shards: 0,
        nodes: 0,
        withSlots: 0,
        migrating: 0,
    });
    const [loading, setLoading] = useState<boolean>(true);
    const [deletingShardIndex, setDeletingShardIndex] = useState<number | null>(null);
    const [searchTerm, setSearchTerm] = useState<string>("");
    const [filterAnchorEl, setFilterAnchorEl] = useState<null | HTMLElement>(null);
    const [sortAnchorEl, setSortAnchorEl] = useState<null | HTMLElement>(null);
    const [filterOption, setFilterOption] = useState<FilterOption>("all");
    const [sortOption, setSortOption] = useState<SortOption>("index-asc");
    const [expandedSlots, setExpandedSlots] = useState<Set<number>>(new Set());
    const [migrationDialogOpen, setMigrationDialogOpen] = useState<boolean>(false);
    const router = useRouter();

    const isActiveMigration = (migratingSlot: string | null | undefined): boolean => {
        return (
            migratingSlot !== null &&
            migratingSlot !== undefined &&
            migratingSlot !== "" &&
            migratingSlot !== "-1"
        );
    };

    const isActiveImport = (importingSlot: string | null | undefined): boolean => {
        return (
            importingSlot !== null &&
            importingSlot !== undefined &&
            importingSlot !== "" &&
            importingSlot !== "-1"
        );
    };

    useEffect(() => {
        const fetchData = async () => {
            try {
                const fetchedShards = await listShards(namespace, cluster);

                if (!fetchedShards) {
                    console.error(`Shards not found`);
                    router.push("/404");
                    return;
                }

                let totalNodes = 0;
                let withSlots = 0;
                let migrating = 0;

                const processedShards = await Promise.all(
                    fetchedShards.map(async (shard: any, index: number) => {
                        const nodeCount = shard.nodes?.length || 0;
                        totalNodes += nodeCount;

                        const hasSlots = shard.slot_ranges && shard.slot_ranges.length > 0;
                        if (hasSlots) withSlots++;

                        // Handle string values from API as per documentation
                        const migratingSlot = shard.migrating_slot || "";
                        const importingSlot = shard.import_slot || "";

                        const hasMigration = isActiveMigration(migratingSlot);
                        if (hasMigration) migrating++;

                        return {
                            index,
                            nodes: shard.nodes || [],
                            slotRanges: shard.slot_ranges || [],
                            migratingSlot,
                            importingSlot,
                            targetShardIndex: shard.target_shard_index || -1,
                            nodeCount,
                            hasSlots,
                            hasMigration,
                            hasImporting: isActiveImport(importingSlot),
                        };
                    })
                );

                setShardsData(processedShards);
                setResourceCounts({
                    shards: processedShards.length,
                    nodes: totalNodes,
                    withSlots,
                    migrating,
                });
            } catch (error) {
                console.error("Error fetching shards:", error);
            } finally {
                setLoading(false);
            }
        };
        fetchData();
    }, [namespace, cluster, router]);

    const refreshShardData = async () => {
        setLoading(true);
        try {
            const fetchedShards = await listShards(namespace, cluster);
            if (!fetchedShards) {
                console.error(`Shards not found`);
                router.push("/404");
                return;
            }

            let totalNodes = 0;
            let withSlots = 0;
            let migrating = 0;

            const processedShards = await Promise.all(
                fetchedShards.map(async (shard: any, index: number) => {
                    const nodeCount = shard.nodes?.length || 0;
                    totalNodes += nodeCount;

                    const hasSlots = shard.slot_ranges && shard.slot_ranges.length > 0;
                    if (hasSlots) withSlots++;

                    const migratingSlot = shard.migrating_slot || "";
                    const importingSlot = shard.import_slot || "";

                    const hasMigration = isActiveMigration(migratingSlot);
                    if (hasMigration) migrating++;

                    return {
                        index,
                        nodes: shard.nodes || [],
                        slotRanges: shard.slot_ranges || [],
                        migratingSlot,
                        importingSlot,
                        targetShardIndex: shard.target_shard_index || -1,
                        nodeCount,
                        hasSlots,
                        hasMigration,
                        hasImporting: isActiveImport(importingSlot),
                    };
                })
            );

            setShardsData(processedShards);
            setResourceCounts({
                shards: processedShards.length,
                nodes: totalNodes,
                withSlots,
                migrating,
            });
        } catch (error) {
            console.error("Error fetching shards:", error);
        } finally {
            setLoading(false);
        }
    };

    const handleDeleteShard = async (index: number) => {
        if (
            !confirm(
                `Are you sure you want to delete Shard ${index + 1}? This action cannot be undone.`
            )
        )
            return;

        try {
            setDeletingShardIndex(index);
            const responseMessage = await deleteShard(namespace, cluster, index.toString());
            if (responseMessage && responseMessage !== "") {
                alert(`Failed to delete shard: ${responseMessage}`);
                return;
            }

            const fetchedShards = await listShards(namespace, cluster);
            let totalNodes = 0;
            let withSlots = 0;
            let migrating = 0;

            const processedShards = (fetchedShards || []).map((shard: any, idx: number) => {
                const nodeCount = shard.nodes?.length || 0;
                totalNodes += nodeCount;

                const hasSlots = shard.slot_ranges && shard.slot_ranges.length > 0;
                if (hasSlots) withSlots++;

                const migratingSlot =
                    shard.migrating_slot !== null && shard.migrating_slot !== undefined
                        ? shard.migrating_slot
                        : -1;
                const importingSlot =
                    shard.import_slot !== null && shard.import_slot !== undefined
                        ? shard.import_slot
                        : -1;

                const hasMigration = migratingSlot >= 0;
                if (hasMigration) migrating++;

                return {
                    index: idx,
                    nodes: shard.nodes || [],
                    slotRanges: shard.slot_ranges || [],
                    migratingSlot,
                    importingSlot,
                    targetShardIndex: shard.target_shard_index || -1,
                    nodeCount,
                    hasSlots,
                    hasMigration,
                    hasImporting: importingSlot >= 0,
                } as ShardData;
            });

            setShardsData(processedShards);
            setResourceCounts({
                shards: processedShards.length,
                nodes: totalNodes,
                withSlots,
                migrating,
            });
        } catch (error) {
            alert(`Failed to delete shard: ${error}`);
        } finally {
            setDeletingShardIndex(null);
        }
    };

    const handleFilterClick = (event: React.MouseEvent<HTMLElement>) => {
        setFilterAnchorEl(event.currentTarget);
    };

    const handleSortClick = (event: React.MouseEvent<HTMLElement>) => {
        setSortAnchorEl(event.currentTarget);
    };

    const handleFilterClose = () => {
        setFilterAnchorEl(null);
    };

    const handleSortClose = () => {
        setSortAnchorEl(null);
    };

    const filteredAndSortedShards = shardsData
        .filter((shard) => {
            if (!`shard ${shard.index + 1}`.toLowerCase().includes(searchTerm.toLowerCase())) {
                return false;
            }

            switch (filterOption) {
                case "with-migration":
                    return shard.hasMigration;
                case "no-migration":
                    return !shard.hasMigration;
                case "with-slots":
                    return shard.hasSlots;
                case "no-slots":
                    return !shard.hasSlots;
                case "with-importing":
                    return shard.hasImporting;
                default:
                    return true;
            }
        })
        .sort((a, b) => {
            switch (sortOption) {
                case "index-asc":
                    return a.index - b.index;
                case "index-desc":
                    return b.index - a.index;
                case "nodes-desc":
                    return b.nodeCount - a.nodeCount;
                case "nodes-asc":
                    return a.nodeCount - b.nodeCount;
                default:
                    return 0;
            }
        });

    const isFilterOpen = Boolean(filterAnchorEl);
    const isSortOpen = Boolean(sortAnchorEl);
    const filterId = isFilterOpen ? "filter-popover" : undefined;
    const sortId = isSortOpen ? "sort-popover" : undefined;

    if (loading) {
        return <LoadingSpinner />;
    }

    const formatSlotRanges = (ranges: string[], showAll: boolean = false) => {
        if (!ranges || ranges.length === 0) return "None";
        if (showAll || ranges.length <= 2) return ranges.join(", ");
        return `${ranges[0]}, ${ranges[1]}, ... (+${ranges.length - 2} more)`;
    };

    const expandSlotRanges = (ranges: string[]) => {
        if (!ranges || ranges.length === 0) return [];
        const slots: number[] = [];
        for (const range of ranges) {
            if (range.includes("-")) {
                const [start, end] = range.split("-").map(Number);
                if (!isNaN(start) && !isNaN(end)) {
                    for (let i = start; i <= end; i++) {
                        slots.push(i);
                    }
                }
            } else {
                const slot = Number(range);
                if (!isNaN(slot)) {
                    slots.push(slot);
                }
            }
        }
        return slots.sort((a, b) => a - b);
    };

    const toggleSlotExpansion = (shardIndex: number) => {
        const newExpanded = new Set(expandedSlots);
        if (newExpanded.has(shardIndex)) {
            newExpanded.delete(shardIndex);
        } else {
            newExpanded.add(shardIndex);
        }
        setExpandedSlots(newExpanded);
    };

    return (
        <div className="flex h-full">
            <div className="relative h-full">
                <ClusterSidebar namespace={namespace} />
            </div>
            <div className="no-scrollbar flex-1 overflow-y-auto bg-white pb-8 dark:bg-dark">
                <Box className="px-6 py-4 sm:px-8 sm:py-6">
                    <div className="mb-4 flex flex-col gap-3 sm:mb-5 lg:flex-row lg:items-center lg:justify-between">
                        <div>
                            <Typography
                                variant="h4"
                                className="flex items-center font-medium text-gray-900 dark:text-white"
                            >
                                <StorageIcon className="mr-3 text-primary dark:text-primary-light" />
                                {cluster}
                            </Typography>
                            <Typography
                                variant="body1"
                                className="mt-0.5 text-gray-500 dark:text-gray-400"
                            >
                                Manage shards in this cluster
                            </Typography>
                        </div>

                        <div className="flex w-full flex-row items-center gap-2 lg:w-auto">
                            <div className="search-container relative max-w-md flex-grow transition-all duration-300 lg:min-w-[280px]">
                                <div
                                    className="search-inner relative w-full bg-gray-50 transition-all duration-300 focus-within:bg-white focus-within:shadow-md dark:bg-dark-paper/90 dark:focus-within:bg-dark-paper"
                                    style={{ borderRadius: "16px" }}
                                >
                                    <div className="pointer-events-none absolute inset-y-0 left-3 flex items-center">
                                        <SearchIcon
                                            className="text-gray-400"
                                            sx={{ fontSize: 18 }}
                                        />
                                    </div>
                                    <input
                                        type="text"
                                        placeholder="Search shards..."
                                        className="w-full border-0 bg-transparent py-2.5 pl-9 pr-4 text-sm text-gray-800 outline-none ring-1 ring-gray-200 transition-all focus:ring-2 focus:ring-primary dark:text-gray-200 dark:ring-gray-700 dark:focus:ring-primary-light"
                                        style={{ borderRadius: "16px" }}
                                        value={searchTerm}
                                        onChange={(e) => setSearchTerm(e.target.value)}
                                    />
                                    {searchTerm && (
                                        <button
                                            className="absolute inset-y-0 right-3 flex items-center text-gray-400 transition-colors hover:text-gray-600 dark:hover:text-gray-300"
                                            onClick={() => setSearchTerm("")}
                                        >
                                            <span className="text-xs">✕</span>
                                        </button>
                                    )}
                                </div>
                            </div>

                            <div className="flex flex-shrink-0 gap-3">
                                <ShardCreation
                                    position="page"
                                    namespace={namespace}
                                    cluster={cluster}
                                >
                                    <Button
                                        variant="outlined"
                                        color="primary"
                                        className="whitespace-nowrap px-5 py-2.5 font-medium shadow-sm transition-all hover:shadow-md"
                                        startIcon={<AddIcon />}
                                        disableElevation
                                        size="medium"
                                        style={{ borderRadius: "16px" }}
                                    >
                                        Create Shard
                                    </Button>
                                </ShardCreation>
                                <Button
                                    variant="outlined"
                                    color="warning"
                                    className="whitespace-nowrap px-5 py-2.5 font-medium shadow-sm transition-all hover:shadow-md"
                                    startIcon={<MoveUpIcon />}
                                    disableElevation
                                    size="medium"
                                    style={{ borderRadius: "16px" }}
                                    onClick={() => setMigrationDialogOpen(true)}
                                    disabled={
                                        shardsData.filter((shard) => shard.hasSlots).length < 2
                                    }
                                >
                                    Migrate Slot
                                </Button>
                            </div>
                        </div>
                    </div>

                    <div className="mb-4 sm:mb-5">
                        <Grid container spacing={2}>
                            <Grid item xs={12} sm={6} lg={3}>
                                <Paper
                                    elevation={0}
                                    className="relative h-full overflow-hidden border border-gray-100 p-4 transition-all hover:-translate-y-1 hover:shadow-md dark:border-gray-800 dark:bg-dark-paper"
                                    style={{ borderRadius: "20px" }}
                                >
                                    <div className="flex items-center justify-between">
                                        <div
                                            className="flex h-12 w-12 items-center justify-center bg-purple-50 text-purple-500 dark:bg-purple-900/30 dark:text-purple-400"
                                            style={{ borderRadius: "16px" }}
                                        >
                                            <DnsIcon sx={{ fontSize: 24 }} />
                                        </div>
                                        <div className="flex flex-col items-end">
                                            <Typography
                                                variant="h4"
                                                className="font-semibold text-gray-900 dark:text-white"
                                            >
                                                {resourceCounts.shards}
                                            </Typography>
                                            <Typography
                                                variant="body2"
                                                className="text-gray-500 dark:text-gray-400"
                                            >
                                                Shards
                                            </Typography>
                                        </div>
                                    </div>
                                    <div className="absolute -bottom-4 -right-4 h-24 w-24 rounded-full bg-purple-500/5 blur-xl"></div>
                                </Paper>
                            </Grid>

                            <Grid item xs={12} sm={6} lg={3}>
                                <Paper
                                    elevation={0}
                                    className="relative h-full overflow-hidden border border-gray-100 p-4 transition-all hover:-translate-y-1 hover:shadow-md dark:border-gray-800 dark:bg-dark-paper"
                                    style={{ borderRadius: "20px" }}
                                >
                                    <div className="flex items-center justify-between">
                                        <div
                                            className="flex h-12 w-12 items-center justify-center bg-green-50 text-green-500 dark:bg-green-900/30 dark:text-green-400"
                                            style={{ borderRadius: "16px" }}
                                        >
                                            <DeviceHubIcon sx={{ fontSize: 24 }} />
                                        </div>
                                        <div className="flex flex-col items-end">
                                            <Typography
                                                variant="h4"
                                                className="font-semibold text-gray-900 dark:text-white"
                                            >
                                                {resourceCounts.nodes}
                                            </Typography>
                                            <Typography
                                                variant="body2"
                                                className="text-gray-500 dark:text-gray-400"
                                            >
                                                Nodes
                                            </Typography>
                                        </div>
                                    </div>
                                    <div className="absolute -bottom-4 -right-4 h-24 w-24 rounded-full bg-green-500/5 blur-xl"></div>
                                </Paper>
                            </Grid>

                            <Grid item xs={12} sm={6} lg={3}>
                                <Paper
                                    elevation={0}
                                    className="relative h-full overflow-hidden border border-gray-100 p-4 transition-all hover:-translate-y-1 hover:shadow-md dark:border-gray-800 dark:bg-dark-paper"
                                    style={{ borderRadius: "20px" }}
                                >
                                    <div className="flex items-center justify-between">
                                        <div
                                            className="flex h-12 w-12 items-center justify-center bg-amber-50 text-amber-500 dark:bg-amber-900/30 dark:text-amber-400"
                                            style={{ borderRadius: "16px" }}
                                        >
                                            <StorageIcon sx={{ fontSize: 24 }} />
                                        </div>
                                        <div className="flex flex-col items-end">
                                            <Typography
                                                variant="h4"
                                                className="font-semibold text-gray-900 dark:text-white"
                                            >
                                                {resourceCounts.withSlots}
                                            </Typography>
                                            <Typography
                                                variant="body2"
                                                className="text-gray-500 dark:text-gray-400"
                                            >
                                                With Slots
                                            </Typography>
                                        </div>
                                    </div>
                                    <div className="absolute -bottom-4 -right-4 h-24 w-24 rounded-full bg-amber-500/5 blur-xl"></div>
                                </Paper>
                            </Grid>

                            <Grid item xs={12} sm={6} lg={3}>
                                <Paper
                                    elevation={0}
                                    className="relative h-full overflow-hidden border border-gray-100 p-4 transition-all hover:-translate-y-1 hover:shadow-md dark:border-gray-800 dark:bg-dark-paper"
                                    style={{ borderRadius: "20px" }}
                                >
                                    <div className="flex items-center justify-between">
                                        <div
                                            className="flex h-12 w-12 items-center justify-center bg-orange-50 text-orange-500 dark:bg-orange-900/30 dark:text-orange-400"
                                            style={{ borderRadius: "16px" }}
                                        >
                                            <WarningIcon sx={{ fontSize: 24 }} />
                                        </div>
                                        <div className="flex flex-col items-end">
                                            <Typography
                                                variant="h4"
                                                className="font-semibold text-gray-900 dark:text-white"
                                            >
                                                {resourceCounts.migrating}
                                            </Typography>
                                            <Typography
                                                variant="body2"
                                                className="text-gray-500 dark:text-gray-400"
                                            >
                                                Migrating
                                            </Typography>
                                        </div>
                                    </div>
                                    <div className="absolute -bottom-4 -right-4 h-24 w-24 rounded-full bg-orange-500/5 blur-xl"></div>
                                </Paper>
                            </Grid>
                        </Grid>
                    </div>

                    <Paper
                        elevation={0}
                        className="overflow-hidden border border-gray-100 transition-all hover:shadow-md dark:border-gray-800 dark:bg-dark-paper"
                        style={{ borderRadius: "20px" }}
                    >
                        <div className="border-b border-gray-100 px-6 py-3 dark:border-gray-800 sm:px-8">
                            <div className="flex items-center justify-between">
                                <Typography
                                    variant="h6"
                                    className="font-medium text-gray-800 dark:text-gray-100"
                                >
                                    All Shards
                                </Typography>
                                <div className="flex items-center gap-2">
                                    <Tooltip title="Filter">
                                        <IconButton
                                            size="small"
                                            onClick={handleFilterClick}
                                            aria-describedby={filterId}
                                            className="bg-gray-50 text-gray-500 hover:bg-gray-100 dark:bg-gray-800 dark:text-gray-400 dark:hover:bg-gray-700"
                                            style={{ borderRadius: "16px" }}
                                        >
                                            <FilterListIcon fontSize="small" />
                                        </IconButton>
                                    </Tooltip>
                                    <Tooltip title="Sort">
                                        <IconButton
                                            size="small"
                                            onClick={handleSortClick}
                                            aria-describedby={sortId}
                                            className="bg-gray-50 text-gray-500 hover:bg-gray-100 dark:bg-gray-800 dark:text-gray-400 dark:hover:bg-gray-700"
                                            style={{ borderRadius: "16px" }}
                                        >
                                            <SortIcon fontSize="small" />
                                        </IconButton>
                                    </Tooltip>
                                </div>
                            </div>
                        </div>

                        <Popover
                            id={filterId}
                            open={isFilterOpen}
                            anchorEl={filterAnchorEl}
                            onClose={handleFilterClose}
                            anchorOrigin={{
                                vertical: "bottom",
                                horizontal: "right",
                            }}
                            transformOrigin={{
                                vertical: "top",
                                horizontal: "right",
                            }}
                            TransitionComponent={Fade}
                            PaperProps={{
                                className:
                                    "rounded-xl shadow-xl border border-gray-100 dark:border-gray-700",
                                elevation: 3,
                                sx: { width: 280 },
                            }}
                        >
                            <div className="p-4">
                                <div className="mb-3 flex items-center justify-between border-b border-gray-100 pb-2 dark:border-gray-700">
                                    <Typography variant="subtitle1" className="font-medium">
                                        Filter Shards
                                    </Typography>
                                </div>

                                <RadioGroup
                                    value={filterOption}
                                    onChange={(e) =>
                                        setFilterOption(e.target.value as FilterOption)
                                    }
                                >
                                    <div className="space-y-2">
                                        <div className="rounded-lg bg-gray-50 p-2 dark:bg-gray-800">
                                            <FormControlLabel
                                                value="all"
                                                control={
                                                    <Radio
                                                        size="small"
                                                        className="text-primary"
                                                        checkedIcon={
                                                            <div className="flex h-5 w-5 items-center justify-center rounded-full border-2 border-primary bg-primary text-white">
                                                                <CheckIcon
                                                                    style={{ fontSize: 12 }}
                                                                />
                                                            </div>
                                                        }
                                                    />
                                                }
                                                label={
                                                    <div className="flex items-center">
                                                        <span className="text-sm font-medium">
                                                            All shards
                                                        </span>
                                                        <Chip
                                                            size="small"
                                                            label={shardsData.length}
                                                            className="ml-2"
                                                            sx={{ height: 20, fontSize: "0.7rem" }}
                                                        />
                                                    </div>
                                                }
                                                className="m-0 w-full"
                                            />
                                        </div>

                                        <div className="rounded-lg bg-gray-50 p-2 dark:bg-gray-800">
                                            <FormControlLabel
                                                value="with-migration"
                                                control={
                                                    <Radio
                                                        size="small"
                                                        className="text-primary"
                                                        checkedIcon={
                                                            <div className="flex h-5 w-5 items-center justify-center rounded-full border-2 border-primary bg-primary text-white">
                                                                <CheckIcon
                                                                    style={{ fontSize: 12 }}
                                                                />
                                                            </div>
                                                        }
                                                    />
                                                }
                                                label={
                                                    <div className="flex items-center">
                                                        <span className="text-sm font-medium">
                                                            With migration
                                                        </span>
                                                        <Chip
                                                            size="small"
                                                            label={
                                                                shardsData.filter(
                                                                    (shard) => shard.hasMigration
                                                                ).length
                                                            }
                                                            className="ml-2"
                                                            sx={{ height: 20, fontSize: "0.7rem" }}
                                                        />
                                                    </div>
                                                }
                                                className="m-0 w-full"
                                            />
                                        </div>

                                        <div className="rounded-lg bg-gray-50 p-2 dark:bg-gray-800">
                                            <FormControlLabel
                                                value="no-migration"
                                                control={
                                                    <Radio
                                                        size="small"
                                                        className="text-primary"
                                                        checkedIcon={
                                                            <div className="flex h-5 w-5 items-center justify-center rounded-full border-2 border-primary bg-primary text-white">
                                                                <CheckIcon
                                                                    style={{ fontSize: 12 }}
                                                                />
                                                            </div>
                                                        }
                                                    />
                                                }
                                                label={
                                                    <div className="flex items-center">
                                                        <span className="text-sm font-medium">
                                                            No migration
                                                        </span>
                                                        <Chip
                                                            size="small"
                                                            label={
                                                                shardsData.filter(
                                                                    (shard) => !shard.hasMigration
                                                                ).length
                                                            }
                                                            className="ml-2"
                                                            sx={{ height: 20, fontSize: "0.7rem" }}
                                                        />
                                                    </div>
                                                }
                                                className="m-0 w-full"
                                            />
                                        </div>

                                        <div className="rounded-lg bg-gray-50 p-2 dark:bg-gray-800">
                                            <FormControlLabel
                                                value="with-slots"
                                                control={
                                                    <Radio
                                                        size="small"
                                                        className="text-primary"
                                                        checkedIcon={
                                                            <div className="flex h-5 w-5 items-center justify-center rounded-full border-2 border-primary bg-primary text-white">
                                                                <CheckIcon
                                                                    style={{ fontSize: 12 }}
                                                                />
                                                            </div>
                                                        }
                                                    />
                                                }
                                                label={
                                                    <div className="flex items-center">
                                                        <span className="text-sm font-medium">
                                                            With slots
                                                        </span>
                                                        <Chip
                                                            size="small"
                                                            label={
                                                                shardsData.filter(
                                                                    (shard) => shard.hasSlots
                                                                ).length
                                                            }
                                                            className="ml-2"
                                                            sx={{ height: 20, fontSize: "0.7rem" }}
                                                        />
                                                    </div>
                                                }
                                                className="m-0 w-full"
                                            />
                                        </div>

                                        <div className="rounded-lg bg-gray-50 p-2 dark:bg-gray-800">
                                            <FormControlLabel
                                                value="no-slots"
                                                control={
                                                    <Radio
                                                        size="small"
                                                        className="text-primary"
                                                        checkedIcon={
                                                            <div className="flex h-5 w-5 items-center justify-center rounded-full border-2 border-primary bg-primary text-white">
                                                                <CheckIcon
                                                                    style={{ fontSize: 12 }}
                                                                />
                                                            </div>
                                                        }
                                                    />
                                                }
                                                label={
                                                    <div className="flex items-center">
                                                        <span className="text-sm font-medium">
                                                            No slots
                                                        </span>
                                                        <Chip
                                                            size="small"
                                                            label={
                                                                shardsData.filter(
                                                                    (shard) => !shard.hasSlots
                                                                ).length
                                                            }
                                                            className="ml-2"
                                                            sx={{ height: 20, fontSize: "0.7rem" }}
                                                        />
                                                    </div>
                                                }
                                                className="m-0 w-full"
                                            />
                                        </div>

                                        <div className="rounded-lg bg-gray-50 p-2 dark:bg-gray-800">
                                            <FormControlLabel
                                                value="with-importing"
                                                control={
                                                    <Radio
                                                        size="small"
                                                        className="text-primary"
                                                        checkedIcon={
                                                            <div className="flex h-5 w-5 items-center justify-center rounded-full border-2 border-primary bg-primary text-white">
                                                                <CheckIcon
                                                                    style={{ fontSize: 12 }}
                                                                />
                                                            </div>
                                                        }
                                                    />
                                                }
                                                label={
                                                    <div className="flex items-center">
                                                        <span className="text-sm font-medium">
                                                            With importing
                                                        </span>
                                                        <Chip
                                                            size="small"
                                                            label={
                                                                shardsData.filter(
                                                                    (shard) => shard.hasImporting
                                                                ).length
                                                            }
                                                            className="ml-2"
                                                            sx={{ height: 20, fontSize: "0.7rem" }}
                                                        />
                                                    </div>
                                                }
                                                className="m-0 w-full"
                                            />
                                        </div>
                                    </div>
                                </RadioGroup>

                                <div className="mt-4 flex justify-end">
                                    <Button
                                        variant="text"
                                        size="small"
                                        onClick={handleFilterClose}
                                        className="px-3 py-1 text-xs"
                                        style={{ borderRadius: "12px" }}
                                    >
                                        Close
                                    </Button>
                                </div>
                            </div>
                        </Popover>

                        <Popover
                            id={sortId}
                            open={isSortOpen}
                            anchorEl={sortAnchorEl}
                            onClose={handleSortClose}
                            anchorOrigin={{
                                vertical: "bottom",
                                horizontal: "right",
                            }}
                            transformOrigin={{
                                vertical: "top",
                                horizontal: "right",
                            }}
                            TransitionComponent={Fade}
                            PaperProps={{
                                className:
                                    "rounded-xl shadow-xl border border-gray-100 dark:border-gray-700",
                                elevation: 3,
                                sx: { width: 280 },
                            }}
                        >
                            <div className="p-4">
                                <div className="mb-3 flex items-center justify-between border-b border-gray-100 pb-2 dark:border-gray-700">
                                    <Typography variant="subtitle1" className="font-medium">
                                        Sort Shards
                                    </Typography>
                                </div>

                                <RadioGroup
                                    value={sortOption}
                                    onChange={(e) => setSortOption(e.target.value as SortOption)}
                                >
                                    <div className="space-y-2">
                                        <div className="rounded-lg bg-gray-50 p-2 dark:bg-gray-800">
                                            <FormControlLabel
                                                value="index-asc"
                                                control={
                                                    <Radio
                                                        size="small"
                                                        className="text-primary"
                                                        checkedIcon={
                                                            <div className="flex h-5 w-5 items-center justify-center rounded-full border-2 border-primary bg-primary text-white">
                                                                <CheckIcon
                                                                    style={{ fontSize: 12 }}
                                                                />
                                                            </div>
                                                        }
                                                    />
                                                }
                                                label={
                                                    <div className="flex items-center">
                                                        <ArrowUpwardIcon
                                                            style={{ fontSize: 16 }}
                                                            className="mr-1 text-gray-500"
                                                        />
                                                        <span className="text-sm font-medium">
                                                            Index 1-{shardsData.length}
                                                        </span>
                                                    </div>
                                                }
                                                className="m-0 w-full"
                                            />
                                        </div>

                                        <div className="rounded-lg bg-gray-50 p-2 dark:bg-gray-800">
                                            <FormControlLabel
                                                value="index-desc"
                                                control={
                                                    <Radio
                                                        size="small"
                                                        className="text-primary"
                                                        checkedIcon={
                                                            <div className="flex h-5 w-5 items-center justify-center rounded-full border-2 border-primary bg-primary text-white">
                                                                <CheckIcon
                                                                    style={{ fontSize: 12 }}
                                                                />
                                                            </div>
                                                        }
                                                    />
                                                }
                                                label={
                                                    <div className="flex items-center">
                                                        <ArrowDownwardIcon
                                                            style={{ fontSize: 16 }}
                                                            className="mr-1 text-gray-500"
                                                        />
                                                        <span className="text-sm font-medium">
                                                            Index {shardsData.length}-1
                                                        </span>
                                                    </div>
                                                }
                                                className="m-0 w-full"
                                            />
                                        </div>

                                        <div className="rounded-lg bg-gray-50 p-2 dark:bg-gray-800">
                                            <FormControlLabel
                                                value="nodes-desc"
                                                control={
                                                    <Radio
                                                        size="small"
                                                        className="text-primary"
                                                        checkedIcon={
                                                            <div className="flex h-5 w-5 items-center justify-center rounded-full border-2 border-primary bg-primary text-white">
                                                                <CheckIcon
                                                                    style={{ fontSize: 12 }}
                                                                />
                                                            </div>
                                                        }
                                                    />
                                                }
                                                label={
                                                    <div className="flex items-center">
                                                        <ArrowDownwardIcon
                                                            style={{ fontSize: 16 }}
                                                            className="mr-1 text-gray-500"
                                                        />
                                                        <span className="text-sm font-medium">
                                                            Most nodes
                                                        </span>
                                                    </div>
                                                }
                                                className="m-0 w-full"
                                            />
                                        </div>

                                        <div className="rounded-lg bg-gray-50 p-2 dark:bg-gray-800">
                                            <FormControlLabel
                                                value="nodes-asc"
                                                control={
                                                    <Radio
                                                        size="small"
                                                        className="text-primary"
                                                        checkedIcon={
                                                            <div className="flex h-5 w-5 items-center justify-center rounded-full border-2 border-primary bg-primary text-white">
                                                                <CheckIcon
                                                                    style={{ fontSize: 12 }}
                                                                />
                                                            </div>
                                                        }
                                                    />
                                                }
                                                label={
                                                    <div className="flex items-center">
                                                        <ArrowUpwardIcon
                                                            style={{ fontSize: 16 }}
                                                            className="mr-1 text-gray-500"
                                                        />
                                                        <span className="text-sm font-medium">
                                                            Least nodes
                                                        </span>
                                                    </div>
                                                }
                                                className="m-0 w-full"
                                            />
                                        </div>
                                    </div>
                                </RadioGroup>

                                <div className="mt-4 flex justify-end">
                                    <Button
                                        variant="text"
                                        size="small"
                                        onClick={handleSortClose}
                                        className="px-3 py-1 text-xs"
                                        style={{ borderRadius: "12px" }}
                                    >
                                        Close
                                    </Button>
                                </div>
                            </div>
                        </Popover>

                        {filteredAndSortedShards.length > 0 ? (
                            <div className="divide-y divide-gray-100 dark:divide-gray-800">
                                {filteredAndSortedShards.map((shard) => (
                                    <div
                                        key={shard.index}
                                        className="group p-2 transition-colors hover:bg-gray-50 dark:hover:bg-gray-800/30"
                                    >
                                        <Paper
                                            elevation={0}
                                            className="overflow-hidden border border-transparent bg-white p-4 transition-all group-hover:border-primary/10 group-hover:shadow-sm dark:bg-dark-paper dark:group-hover:border-primary-dark/20"
                                            style={{ borderRadius: "20px" }}
                                        >
                                            <div className="flex flex-col items-start sm:flex-row sm:items-center">
                                                <div
                                                    className="mb-3 flex h-14 w-14 flex-shrink-0 items-center justify-center bg-green-50 text-green-500 dark:bg-green-900/30 dark:text-green-400 sm:mb-0"
                                                    style={{ borderRadius: "16px" }}
                                                >
                                                    <DnsIcon sx={{ fontSize: 28 }} />
                                                </div>

                                                <div className="flex flex-1 flex-col sm:ml-5 sm:flex-row sm:items-center sm:overflow-hidden">
                                                    <div className="flex-1 overflow-hidden">
                                                        <Link
                                                            href={`/namespaces/${namespace}/clusters/${cluster}/shards/${shard.index}`}
                                                            className="block"
                                                        >
                                                            <div className="flex items-center gap-2">
                                                                <Typography
                                                                    variant="h6"
                                                                    className="truncate font-medium text-gray-900 transition-colors hover:text-primary dark:text-gray-100 dark:hover:text-primary-light"
                                                                >
                                                                    Shard {shard.index + 1}
                                                                </Typography>

                                                                {shard.hasMigration && (
                                                                    <div
                                                                        className="flex items-center gap-1 border border-orange-200 bg-orange-50 px-2.5 py-1 dark:border-orange-800 dark:bg-orange-900/30"
                                                                        style={{
                                                                            borderRadius: "12px",
                                                                        }}
                                                                    >
                                                                        <div className="h-1.5 w-1.5 animate-pulse rounded-full bg-orange-500"></div>
                                                                        <span className="text-xs font-medium text-orange-700 dark:text-orange-300">
                                                                            Migrating{" "}
                                                                            {shard.migratingSlot}
                                                                        </span>
                                                                    </div>
                                                                )}

                                                                {!shard.hasMigration &&
                                                                    !shard.hasImporting && (
                                                                        <div
                                                                            className="flex items-center gap-1 border border-green-200 bg-green-50 px-2.5 py-1 dark:border-green-800 dark:bg-green-900/30"
                                                                            style={{
                                                                                borderRadius:
                                                                                    "12px",
                                                                            }}
                                                                        >
                                                                            <div className="h-1.5 w-1.5 rounded-full bg-green-500"></div>
                                                                            <span className="text-xs font-medium text-green-700 dark:text-green-300">
                                                                                Stable
                                                                            </span>
                                                                        </div>
                                                                    )}

                                                                {shard.hasImporting && (
                                                                    <div
                                                                        className="flex items-center gap-1 border border-blue-200 bg-blue-50 px-2.5 py-1 dark:border-blue-800 dark:bg-blue-900/30"
                                                                        style={{
                                                                            borderRadius: "12px",
                                                                        }}
                                                                    >
                                                                        <div className="h-1.5 w-1.5 animate-pulse rounded-full bg-blue-500"></div>
                                                                        <span className="text-xs font-medium text-blue-700 dark:text-blue-300">
                                                                            Importing{" "}
                                                                            {shard.importingSlot}
                                                                        </span>
                                                                    </div>
                                                                )}
                                                            </div>

                                                            <div className="mt-2 space-y-1">
                                                                <Typography
                                                                    variant="body2"
                                                                    className="flex items-center text-gray-500 dark:text-gray-400"
                                                                >
                                                                    <DeviceHubIcon
                                                                        sx={{ fontSize: 14 }}
                                                                        className="mr-1"
                                                                    />
                                                                    {shard.nodeCount} nodes
                                                                </Typography>

                                                                {shard.hasSlots ? (
                                                                    <div>
                                                                        <div className="flex items-center">
                                                                            <Typography
                                                                                variant="body2"
                                                                                className="flex items-center text-gray-500 dark:text-gray-400"
                                                                            >
                                                                                <StorageIcon
                                                                                    sx={{
                                                                                        fontSize: 14,
                                                                                    }}
                                                                                    className="mr-1"
                                                                                />
                                                                                Slots:{" "}
                                                                                {formatSlotRanges(
                                                                                    shard.slotRanges
                                                                                )}
                                                                            </Typography>
                                                                            <Chip
                                                                                size="small"
                                                                                label={`${shard.slotRanges.length} range${shard.slotRanges.length !== 1 ? "s" : ""}`}
                                                                                color="primary"
                                                                                variant="outlined"
                                                                                className="ml-2"
                                                                                sx={{
                                                                                    height: 20,
                                                                                    fontSize:
                                                                                        "0.7rem",
                                                                                }}
                                                                            />
                                                                            {shard.slotRanges
                                                                                .length > 2 && (
                                                                                <IconButton
                                                                                    size="small"
                                                                                    onClick={(
                                                                                        e
                                                                                    ) => {
                                                                                        e.preventDefault();
                                                                                        e.stopPropagation();
                                                                                        toggleSlotExpansion(
                                                                                            shard.index
                                                                                        );
                                                                                    }}
                                                                                    className="ml-1 p-1"
                                                                                    sx={{
                                                                                        fontSize: 16,
                                                                                    }}
                                                                                >
                                                                                    {expandedSlots.has(
                                                                                        shard.index
                                                                                    ) ? (
                                                                                        <ExpandLessIcon fontSize="small" />
                                                                                    ) : (
                                                                                        <ExpandMoreIcon fontSize="small" />
                                                                                    )}
                                                                                </IconButton>
                                                                            )}
                                                                        </div>
                                                                        <Collapse
                                                                            in={expandedSlots.has(
                                                                                shard.index
                                                                            )}
                                                                        >
                                                                            <div className="mt-2 rounded-lg bg-gray-50 p-2 dark:bg-gray-800/50">
                                                                                <Typography
                                                                                    variant="caption"
                                                                                    className="mb-1 block font-medium text-gray-600 dark:text-gray-300"
                                                                                >
                                                                                    All Slot Ranges
                                                                                    (
                                                                                    {
                                                                                        expandSlotRanges(
                                                                                            shard.slotRanges
                                                                                        ).length
                                                                                    }{" "}
                                                                                    total slots):
                                                                                </Typography>
                                                                                <Typography
                                                                                    variant="body2"
                                                                                    className="text-gray-700 dark:text-gray-200"
                                                                                    sx={{
                                                                                        fontFamily:
                                                                                            "monospace",
                                                                                    }}
                                                                                >
                                                                                    {shard.slotRanges.join(
                                                                                        ", "
                                                                                    )}
                                                                                </Typography>
                                                                            </div>
                                                                        </Collapse>
                                                                    </div>
                                                                ) : (
                                                                    <Typography
                                                                        variant="body2"
                                                                        className="flex items-center text-gray-400 dark:text-gray-500"
                                                                    >
                                                                        <StorageIcon
                                                                            sx={{ fontSize: 14 }}
                                                                            className="mr-1"
                                                                        />
                                                                        No slots assigned
                                                                    </Typography>
                                                                )}

                                                                {shard.targetShardIndex >= 0 && (
                                                                    <Typography
                                                                        variant="body2"
                                                                        className="flex items-center text-amber-600 dark:text-amber-400"
                                                                    >
                                                                        <InfoIcon
                                                                            sx={{ fontSize: 14 }}
                                                                            className="mr-1"
                                                                        />
                                                                        Target shard:{" "}
                                                                        {shard.targetShardIndex + 1}
                                                                    </Typography>
                                                                )}
                                                            </div>
                                                        </Link>
                                                    </div>

                                                    <div className="mt-3 flex space-x-2 overflow-x-auto sm:ml-6 sm:mt-0 md:hidden lg:flex xl:hidden 2xl:flex">
                                                        <Chip
                                                            icon={
                                                                <DeviceHubIcon fontSize="small" />
                                                            }
                                                            label={`${shard.nodeCount} nodes`}
                                                            size="small"
                                                            color="secondary"
                                                            variant="outlined"
                                                            className="whitespace-nowrap"
                                                        />

                                                        {shard.hasSlots ? (
                                                            <Chip
                                                                icon={
                                                                    <StorageIcon fontSize="small" />
                                                                }
                                                                label={`${shard.slotRanges.length} slot${shard.slotRanges.length !== 1 ? "s" : ""}`}
                                                                size="small"
                                                                color="primary"
                                                                variant="outlined"
                                                                className="whitespace-nowrap"
                                                            />
                                                        ) : (
                                                            <Chip
                                                                icon={<InfoIcon fontSize="small" />}
                                                                label="No slots"
                                                                size="small"
                                                                color="info"
                                                                variant="outlined"
                                                                className="whitespace-nowrap"
                                                            />
                                                        )}

                                                        {shard.hasMigration && (
                                                            <Chip
                                                                icon={
                                                                    <WarningIcon fontSize="small" />
                                                                }
                                                                label={`Migrating ${shard.migratingSlot}`}
                                                                size="small"
                                                                color="warning"
                                                                variant="outlined"
                                                                className="whitespace-nowrap"
                                                            />
                                                        )}

                                                        {shard.hasImporting && (
                                                            <Chip
                                                                icon={<InfoIcon fontSize="small" />}
                                                                label={`Importing ${shard.importingSlot}`}
                                                                size="small"
                                                                color="info"
                                                                variant="outlined"
                                                                className="whitespace-nowrap"
                                                            />
                                                        )}

                                                        {shard.targetShardIndex >= 0 && (
                                                            <Chip
                                                                label={`Target: ${shard.targetShardIndex + 1}`}
                                                                size="small"
                                                                color="primary"
                                                                variant="outlined"
                                                                className="whitespace-nowrap"
                                                            />
                                                        )}
                                                    </div>

                                                    <div className="hidden space-x-3 sm:ml-8 md:flex lg:hidden xl:flex 2xl:hidden">
                                                        <div className="flex flex-col items-center rounded-lg border border-gray-100 bg-gray-50 px-4 py-2 dark:border-gray-800 dark:bg-gray-800/50">
                                                            <Typography
                                                                variant="caption"
                                                                className="text-gray-500 dark:text-gray-400"
                                                            >
                                                                Nodes
                                                            </Typography>
                                                            <Typography
                                                                variant="subtitle1"
                                                                className="font-semibold text-gray-900 dark:text-white"
                                                            >
                                                                {shard.nodeCount}
                                                            </Typography>
                                                        </div>

                                                        <div className="flex flex-col items-center rounded-lg border border-gray-100 bg-gray-50 px-4 py-2 dark:border-gray-800 dark:bg-gray-800/50">
                                                            <Typography
                                                                variant="caption"
                                                                className="text-gray-500 dark:text-gray-400"
                                                            >
                                                                Slots
                                                            </Typography>
                                                            <Typography
                                                                variant="subtitle1"
                                                                className="font-semibold text-gray-900 dark:text-white"
                                                            >
                                                                {shard.hasSlots
                                                                    ? shard.slotRanges.length
                                                                    : 0}
                                                            </Typography>
                                                        </div>

                                                        <div className="flex flex-col items-center rounded-lg border border-gray-100 bg-gray-50 px-4 py-2 dark:border-gray-800 dark:bg-gray-800/50">
                                                            <Typography
                                                                variant="caption"
                                                                className="text-gray-500 dark:text-gray-400"
                                                            >
                                                                Status
                                                            </Typography>
                                                            <Typography
                                                                variant="subtitle1"
                                                                className="font-semibold text-gray-900 dark:text-white"
                                                            >
                                                                {shard.hasMigration
                                                                    ? "Migrating"
                                                                    : shard.hasImporting
                                                                      ? "Importing"
                                                                      : "Stable"}
                                                            </Typography>
                                                        </div>
                                                    </div>

                                                    <div className="ml-2 mt-3 flex items-center space-x-2 sm:mt-0">
                                                        <button
                                                            onClick={() =>
                                                                handleDeleteShard(shard.index)
                                                            }
                                                            disabled={
                                                                deletingShardIndex === shard.index
                                                            }
                                                            className="bg-gray-100 p-2 text-gray-600 transition-colors hover:bg-red-100 hover:text-red-600 disabled:cursor-not-allowed disabled:opacity-50 dark:bg-gray-800 dark:text-gray-300 dark:hover:bg-red-900/30 dark:hover:text-red-400"
                                                            style={{ borderRadius: "16px" }}
                                                        >
                                                            <DeleteIcon />
                                                        </button>
                                                        <Link
                                                            href={`/namespaces/${namespace}/clusters/${cluster}/shards/${shard.index}`}
                                                            className="rounded-full bg-primary/10 p-2 text-primary transition-colors hover:bg-primary/20 dark:bg-primary-dark/20 dark:text-primary-light dark:hover:bg-primary-dark/30"
                                                        >
                                                            <ChevronRightIcon />
                                                        </Link>
                                                    </div>
                                                </div>
                                            </div>
                                        </Paper>
                                    </div>
                                ))}
                            </div>
                        ) : (
                            <div className="p-12">
                                <EmptyState
                                    title={
                                        filterOption !== "all"
                                            ? "No matching shards"
                                            : "No shards found"
                                    }
                                    description={
                                        filterOption !== "all"
                                            ? "Try changing your filter settings"
                                            : searchTerm
                                              ? "Try adjusting your search term"
                                              : "Create a shard to get started"
                                    }
                                    icon={<DnsIcon sx={{ fontSize: 64 }} />}
                                    action={{
                                        label: "Create Shard",
                                        onClick: () => {},
                                    }}
                                />
                                <div className="hidden">
                                    <ShardCreation
                                        position="card"
                                        namespace={namespace}
                                        cluster={cluster}
                                    />
                                </div>
                            </div>
                        )}

                        {filteredAndSortedShards.length > 0 && (
                            <div className="bg-gray-50 px-6 py-4 dark:bg-gray-800/30 sm:px-8">
                                <Typography
                                    variant="body2"
                                    className="text-gray-500 dark:text-gray-400"
                                >
                                    Showing {filteredAndSortedShards.length} of {shardsData.length}{" "}
                                    shards
                                </Typography>
                            </div>
                        )}
                    </Paper>
                </Box>
            </div>

            <MigrationDialog
                open={migrationDialogOpen}
                onClose={() => setMigrationDialogOpen(false)}
                namespace={namespace}
                cluster={cluster}
                shards={shardsData}
                onSuccess={refreshShardData}
            />
        </div>
    );
}
