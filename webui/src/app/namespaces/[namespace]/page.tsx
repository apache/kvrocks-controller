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
} from "@mui/material";
import { NamespaceSidebar } from "../../ui/sidebar";
import { fetchCluster, fetchClusters, fetchNamespaces, listShards, listNodes } from "@/app/lib/api";
import Link from "next/link";
import { useRouter, notFound } from "next/navigation";
import { useState, useEffect } from "react";
import { LoadingSpinner } from "@/app/ui/loadingSpinner";
import StorageIcon from "@mui/icons-material/Storage";
import FolderIcon from "@mui/icons-material/Folder";
import EmptyState from "@/app/ui/emptyState";
import GridViewIcon from "@mui/icons-material/GridView";

import SearchIcon from "@mui/icons-material/Search";
import FilterListIcon from "@mui/icons-material/FilterList";
import SortIcon from "@mui/icons-material/Sort";
import CheckIcon from "@mui/icons-material/Check";
import ArrowUpwardIcon from "@mui/icons-material/ArrowUpward";
import ArrowDownwardIcon from "@mui/icons-material/ArrowDownward";
import DnsIcon from "@mui/icons-material/Dns";
import DeviceHubIcon from "@mui/icons-material/DeviceHub";
import ChevronRightIcon from "@mui/icons-material/ChevronRight";
import AccessTimeIcon from "@mui/icons-material/AccessTime";
import AddIcon from "@mui/icons-material/Add";
import FileUploadIcon from "@mui/icons-material/FileUpload";
import WarningIcon from "@mui/icons-material/Warning";
import InfoIcon from "@mui/icons-material/Info";
import { ClusterCreation, ImportCluster } from "@/app/ui/formCreation";

interface ResourceCounts {
    clusters: number;
    shards: number;
    nodes: number;
}

interface ClusterData {
    name: string;
    version: string;
    shards: any[];
    shardCount: number;
    nodeCount: number;
    hasSlots: boolean;
    hasMigration: boolean;
    hasNoMigration: boolean;
    hasImporting: boolean;
    slotRanges: string[];
    migratingSlot: number;
    importingSlot: number;
    targetShardIndex: number;
}

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
    | "nodes-asc";

export default function Namespace({ params }: { params: { namespace: string } }) {
    const [clusterData, setClusterData] = useState<ClusterData[]>([]);
    const [resourceCounts, setResourceCounts] = useState<ResourceCounts>({
        clusters: 0,
        shards: 0,
        nodes: 0,
    });
    const [loading, setLoading] = useState<boolean>(true);
    const [searchTerm, setSearchTerm] = useState<string>("");
    const [filterAnchorEl, setFilterAnchorEl] = useState<null | HTMLElement>(null);
    const [sortAnchorEl, setSortAnchorEl] = useState<null | HTMLElement>(null);
    const [filterOption, setFilterOption] = useState<FilterOption>("all");
    const [sortOption, setSortOption] = useState<SortOption>("name-asc");
    const router = useRouter();

    useEffect(() => {
        const fetchData = async () => {
            try {
                const fetchedNamespaces = await fetchNamespaces();

                if (!fetchedNamespaces.includes(params.namespace)) {
                    console.error(`Namespace ${params.namespace} not found`);
                    notFound();
                    return;
                }

                const clusters = await fetchClusters(params.namespace);
                let totalShards = 0;
                let totalNodes = 0;

                const data = await Promise.all(
                    clusters.map(async (cluster) => {
                        try {
                            const clusterInfo = await fetchCluster(params.namespace, cluster);
                            if (
                                clusterInfo &&
                                typeof clusterInfo === "object" &&
                                "shards" in clusterInfo
                            ) {
                                const shards = (clusterInfo as any).shards || [];

                                let clusterNodeCount = 0;
                                for (let i = 0; i < shards.length; i++) {
                                    try {
                                        const nodes = await listNodes(
                                            params.namespace,
                                            cluster,
                                            i.toString()
                                        );
                                        if (Array.isArray(nodes)) {
                                            clusterNodeCount += nodes.length;
                                        }
                                    } catch (error) {
                                        console.error(
                                            `Failed to fetch nodes for shard ${i}:`,
                                            error
                                        );
                                    }
                                }

                                totalShards += shards.length;
                                totalNodes += clusterNodeCount;

                                const hasSlots = shards.some(
                                    (s: any) => s.slot_ranges && s.slot_ranges.length > 0
                                );
                                const hasMigration = shards.some((s: any) => s.migrating_slot >= 0);
                                const hasNoMigration = shards.every(
                                    (s: any) => s.migrating_slot === -1
                                );
                                const hasImporting = shards.some((s: any) => s.import_slot >= 0);

                                const slotRanges =
                                    shards.find(
                                        (s: any) => s.slot_ranges && s.slot_ranges.length > 0
                                    )?.slot_ranges || [];

                                const migratingSlot =
                                    shards.find((s: any) => s.migrating_slot >= 0)
                                        ?.migrating_slot || -1;
                                const importingSlot =
                                    shards.find((s: any) => s.import_slot >= 0)?.import_slot || -1;
                                const targetShardIndex =
                                    shards.find((s: any) => s.target_shard_index >= 0)
                                        ?.target_shard_index || -1;

                                return {
                                    ...clusterInfo,
                                    shards,
                                    shardCount: shards.length,
                                    nodeCount: clusterNodeCount,
                                    hasSlots,
                                    hasMigration,
                                    hasNoMigration,
                                    hasImporting,
                                    slotRanges,
                                    migratingSlot,
                                    importingSlot,
                                    targetShardIndex,
                                };
                            }
                            return null;
                        } catch (error) {
                            console.error(`Failed to fetch data for cluster ${cluster}:`, error);
                            return null;
                        }
                    })
                );

                const validData = data.filter(Boolean) as ClusterData[];
                setClusterData(validData);
                setResourceCounts({
                    clusters: validData.length,
                    shards: totalShards,
                    nodes: totalNodes,
                });
            } catch (error) {
                console.error("Error fetching data:", error);
            } finally {
                setLoading(false);
            }
        };

        fetchData();
    }, [params.namespace, router]);

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

    const filteredAndSortedClusters = clusterData
        .filter((cluster) => {
            if (!cluster.name.toLowerCase().includes(searchTerm.toLowerCase())) {
                return false;
            }

            switch (filterOption) {
                case "with-migration":
                    return cluster.hasMigration;
                case "no-migration":
                    return !cluster.hasMigration;
                case "with-slots":
                    return cluster.hasSlots;
                case "no-slots":
                    return !cluster.hasSlots;
                case "with-importing":
                    return cluster.hasImporting;
                default:
                    return true;
            }
        })
        .sort((a, b) => {
            switch (sortOption) {
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

    return (
        <div className="flex h-full">
            <div className="relative h-full">
                <NamespaceSidebar />
            </div>
            <div className="no-scrollbar flex-1 overflow-y-auto bg-white pb-8 dark:bg-dark">
                <Box className="px-6 py-4 sm:px-8 sm:py-6">
                    <div className="mb-4 flex flex-col gap-3 sm:mb-5 lg:flex-row lg:items-center lg:justify-between">
                        <div>
                            <Typography
                                variant="h4"
                                className="flex items-center font-medium text-gray-900 dark:text-white"
                            >
                                <FolderIcon className="mr-3 text-primary dark:text-primary-light" />
                                {params.namespace}
                            </Typography>
                            <Typography
                                variant="body1"
                                className="mt-0.5 text-gray-500 dark:text-gray-400"
                            >
                                Manage clusters in this namespace
                            </Typography>
                        </div>

                        <div className="flex w-full flex-row items-center gap-2 lg:w-auto">
                            <div className="search-container relative max-w-md flex-grow transition-all duration-300 lg:min-w-[280px]">
                                <div className="search-inner relative w-full rounded-lg bg-gray-50 transition-all duration-300 focus-within:bg-white focus-within:shadow-md dark:bg-dark-paper/90 dark:focus-within:bg-dark-paper">
                                    <div className="pointer-events-none absolute inset-y-0 left-3 flex items-center">
                                        <SearchIcon
                                            className="text-gray-400"
                                            sx={{ fontSize: 18 }}
                                        />
                                    </div>
                                    <input
                                        type="text"
                                        placeholder="Search clusters..."
                                        className="w-full rounded-lg border-0 bg-transparent py-2.5 pl-9 pr-4 text-sm text-gray-800 outline-none ring-1 ring-gray-200 transition-all focus:ring-2 focus:ring-primary dark:text-gray-200 dark:ring-gray-700 dark:focus:ring-primary-light"
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
                                <ClusterCreation position="page" namespace={params.namespace}>
                                    <Button
                                        variant="outlined"
                                        color="primary"
                                        className="whitespace-nowrap rounded-lg px-5 py-2.5 font-medium shadow-sm transition-all hover:shadow-md"
                                        startIcon={<AddIcon />}
                                        disableElevation
                                        size="medium"
                                    >
                                        Create Cluster
                                    </Button>
                                </ClusterCreation>
                                <ImportCluster position="page" namespace={params.namespace}>
                                    <Button
                                        variant="outlined"
                                        color="primary"
                                        className="whitespace-nowrap rounded-lg px-5 py-2.5 font-medium shadow-sm transition-all hover:shadow-md"
                                        startIcon={<FileUploadIcon />}
                                        disableElevation
                                        size="medium"
                                    >
                                        Import Cluster
                                    </Button>
                                </ImportCluster>
                            </div>
                        </div>
                    </div>

                    <div className="mb-4 sm:mb-5">
                        <Grid container spacing={2}>
                            <Grid item xs={12} sm={6} lg={3}>
                                <Paper
                                    elevation={0}
                                    className="relative h-full overflow-hidden rounded-2xl border border-gray-100 p-4 transition-all hover:-translate-y-1 hover:shadow-md dark:border-gray-800 dark:bg-dark-paper"
                                >
                                    <div className="flex items-center justify-between">
                                        <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-purple-50 text-purple-500 dark:bg-purple-900/30 dark:text-purple-400">
                                            <StorageIcon sx={{ fontSize: 24 }} />
                                        </div>
                                        <div className="flex flex-col items-end">
                                            <Typography
                                                variant="h4"
                                                className="font-semibold text-gray-900 dark:text-white"
                                            >
                                                {resourceCounts.clusters}
                                            </Typography>
                                            <Typography
                                                variant="body2"
                                                className="text-gray-500 dark:text-gray-400"
                                            >
                                                Clusters
                                            </Typography>
                                        </div>
                                    </div>
                                    <div className="absolute -bottom-4 -right-4 h-24 w-24 rounded-full bg-purple-500/5 blur-xl"></div>
                                </Paper>
                            </Grid>

                            <Grid item xs={12} sm={6} lg={3}>
                                <Paper
                                    elevation={0}
                                    className="relative h-full overflow-hidden rounded-2xl border border-gray-100 p-4 transition-all hover:-translate-y-1 hover:shadow-md dark:border-gray-800 dark:bg-dark-paper"
                                >
                                    <div className="flex items-center justify-between">
                                        <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-green-50 text-green-500 dark:bg-green-900/30 dark:text-green-400">
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
                                    <div className="absolute -bottom-4 -right-4 h-24 w-24 rounded-full bg-green-500/5 blur-xl"></div>
                                </Paper>
                            </Grid>

                            <Grid item xs={12} sm={6} lg={3}>
                                <Paper
                                    elevation={0}
                                    className="relative h-full overflow-hidden rounded-2xl border border-gray-100 p-4 transition-all hover:-translate-y-1 hover:shadow-md dark:border-gray-800 dark:bg-dark-paper"
                                >
                                    <div className="flex items-center justify-between">
                                        <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-amber-50 text-amber-500 dark:bg-amber-900/30 dark:text-amber-400">
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
                                    <div className="absolute -bottom-4 -right-4 h-24 w-24 rounded-full bg-amber-500/5 blur-xl"></div>
                                </Paper>
                            </Grid>

                            <Grid item xs={12} sm={6} lg={3}>
                                <Paper
                                    elevation={0}
                                    className="relative h-full overflow-hidden rounded-2xl border border-gray-100 p-4 transition-all hover:-translate-y-1 hover:shadow-md dark:border-gray-800 dark:bg-dark-paper"
                                >
                                    <div className="flex items-center justify-between">
                                        <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-indigo-50 text-indigo-500 dark:bg-indigo-900/30 dark:text-indigo-400">
                                            <GridViewIcon sx={{ fontSize: 24 }} />
                                        </div>
                                        <div className="flex flex-col items-end">
                                            <Typography
                                                variant="h4"
                                                className="font-semibold text-gray-900 dark:text-white"
                                            >
                                                {clusterData.filter((c) => c.hasSlots).length}
                                            </Typography>
                                            <Typography
                                                variant="body2"
                                                className="text-gray-500 dark:text-gray-400"
                                            >
                                                With Slots
                                            </Typography>
                                        </div>
                                    </div>
                                    <div className="absolute -bottom-4 -right-4 h-24 w-24 rounded-full bg-indigo-500/5 blur-xl"></div>
                                </Paper>
                            </Grid>
                        </Grid>
                    </div>

                    <Paper
                        elevation={0}
                        className="overflow-hidden rounded-2xl border border-gray-100 transition-all hover:shadow-md dark:border-gray-800 dark:bg-dark-paper"
                    >
                        <div className="border-b border-gray-100 px-6 py-3 dark:border-gray-800 sm:px-8">
                            <div className="flex items-center justify-between">
                                <Typography
                                    variant="h6"
                                    className="font-medium text-gray-800 dark:text-gray-100"
                                >
                                    All Clusters
                                </Typography>
                                <div className="flex items-center gap-2">
                                    <Tooltip title="Filter">
                                        <IconButton
                                            size="small"
                                            onClick={handleFilterClick}
                                            aria-describedby={filterId}
                                            className="rounded-full bg-gray-50 text-gray-500 hover:bg-gray-100 dark:bg-gray-800 dark:text-gray-400 dark:hover:bg-gray-700"
                                        >
                                            <FilterListIcon fontSize="small" />
                                        </IconButton>
                                    </Tooltip>
                                    <Tooltip title="Sort">
                                        <IconButton
                                            size="small"
                                            onClick={handleSortClick}
                                            aria-describedby={sortId}
                                            className="rounded-full bg-gray-50 text-gray-500 hover:bg-gray-100 dark:bg-gray-800 dark:text-gray-400 dark:hover:bg-gray-700"
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
                                        Filter Clusters
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
                                                            All clusters
                                                        </span>
                                                        <Chip
                                                            size="small"
                                                            label={clusterData.length}
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
                                                                clusterData.filter(
                                                                    (cluster) =>
                                                                        cluster.hasMigration
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
                                                                clusterData.filter(
                                                                    (cluster) =>
                                                                        !cluster.hasMigration
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
                                                                clusterData.filter(
                                                                    (cluster) => cluster.hasSlots
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
                                                                clusterData.filter(
                                                                    (cluster) => !cluster.hasSlots
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
                                                                clusterData.filter(
                                                                    (cluster) =>
                                                                        cluster.hasImporting
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
                                        className="rounded-lg px-3 py-1 text-xs"
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
                                        Sort Clusters
                                    </Typography>
                                </div>

                                <RadioGroup
                                    value={sortOption}
                                    onChange={(e) => setSortOption(e.target.value as SortOption)}
                                >
                                    <div className="space-y-2">
                                        <div className="rounded-lg bg-gray-50 p-2 dark:bg-gray-800">
                                            <FormControlLabel
                                                value="name-asc"
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
                                                            Name A-Z
                                                        </span>
                                                    </div>
                                                }
                                                className="m-0 w-full"
                                            />
                                        </div>

                                        <div className="rounded-lg bg-gray-50 p-2 dark:bg-gray-800">
                                            <FormControlLabel
                                                value="name-desc"
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
                                                            Name Z-A
                                                        </span>
                                                    </div>
                                                }
                                                className="m-0 w-full"
                                            />
                                        </div>

                                        <div className="rounded-lg bg-gray-50 p-2 dark:bg-gray-800">
                                            <FormControlLabel
                                                value="shards-desc"
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
                                                            Most shards
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
                                    </div>
                                </RadioGroup>

                                <div className="mt-4 flex justify-end">
                                    <Button
                                        variant="text"
                                        size="small"
                                        onClick={handleSortClose}
                                        className="rounded-lg px-3 py-1 text-xs"
                                    >
                                        Close
                                    </Button>
                                </div>
                            </div>
                        </Popover>

                        {filteredAndSortedClusters.length > 0 ? (
                            <div className="divide-y divide-gray-100 dark:divide-gray-800">
                                {filteredAndSortedClusters.map((cluster) => (
                                    <div
                                        key={cluster.name}
                                        className="group p-2 transition-colors hover:bg-gray-50 dark:hover:bg-gray-800/30"
                                    >
                                        <Paper
                                            elevation={0}
                                            className="overflow-hidden rounded-xl border border-transparent bg-white p-4 transition-all group-hover:border-primary/10 group-hover:shadow-sm dark:bg-dark-paper dark:group-hover:border-primary-dark/20"
                                        >
                                            <div className="flex flex-col items-start sm:flex-row sm:items-center">
                                                <div className="mb-3 flex h-14 w-14 flex-shrink-0 items-center justify-center rounded-xl bg-purple-50 text-purple-500 dark:bg-purple-900/30 dark:text-purple-400 sm:mb-0">
                                                    <StorageIcon sx={{ fontSize: 28 }} />
                                                </div>

                                                <div className="flex flex-1 flex-col sm:ml-5 sm:flex-row sm:items-center sm:overflow-hidden">
                                                    <div className="flex-1 overflow-hidden">
                                                        <Link
                                                            href={`/namespaces/${params.namespace}/clusters/${cluster.name}`}
                                                            className="block"
                                                        >
                                                            <div className="flex items-center gap-2">
                                                                <Typography
                                                                    variant="h6"
                                                                    className="truncate font-medium text-gray-900 transition-colors hover:text-primary dark:text-gray-100 dark:hover:text-primary-light"
                                                                >
                                                                    {cluster.name}
                                                                </Typography>

                                                                {cluster.hasMigration &&
                                                                    cluster.migratingSlot >= 0 && (
                                                                        <div className="flex items-center gap-1 rounded-full border border-orange-200 bg-orange-50 px-2.5 py-1 dark:border-orange-800 dark:bg-orange-900/30">
                                                                            <div className="h-1.5 w-1.5 animate-pulse rounded-full bg-orange-500"></div>
                                                                            <span className="text-xs font-medium text-orange-700 dark:text-orange-300">
                                                                                Migrating{" "}
                                                                                {
                                                                                    cluster.migratingSlot
                                                                                }
                                                                            </span>
                                                                        </div>
                                                                    )}

                                                                {(!cluster.hasMigration ||
                                                                    cluster.migratingSlot ===
                                                                        -1) && (
                                                                    <div className="flex items-center gap-1 rounded-full border border-green-200 bg-green-50 px-2.5 py-1 dark:border-green-800 dark:bg-green-900/30">
                                                                        <div className="h-1.5 w-1.5 rounded-full bg-green-500"></div>
                                                                        <span className="text-xs font-medium text-green-700 dark:text-green-300">
                                                                            Stable
                                                                        </span>
                                                                    </div>
                                                                )}

                                                                {cluster.hasImporting &&
                                                                    cluster.importingSlot >= 0 && (
                                                                        <div className="flex items-center gap-1 rounded-full border border-blue-200 bg-blue-50 px-2.5 py-1 dark:border-blue-800 dark:bg-blue-900/30">
                                                                            <div className="h-1.5 w-1.5 animate-pulse rounded-full bg-blue-500"></div>
                                                                            <span className="text-xs font-medium text-blue-700 dark:text-blue-300">
                                                                                Importing{" "}
                                                                                {
                                                                                    cluster.importingSlot
                                                                                }
                                                                            </span>
                                                                        </div>
                                                                    )}
                                                            </div>

                                                            <Typography
                                                                variant="body2"
                                                                className="flex items-center text-gray-500 dark:text-gray-400"
                                                            >
                                                                <AccessTimeIcon
                                                                    sx={{ fontSize: 14 }}
                                                                    className="mr-1"
                                                                />
                                                                Version: {cluster.version}
                                                            </Typography>
                                                            {cluster.hasSlots && (
                                                                <Typography
                                                                    variant="caption"
                                                                    className="text-gray-400 dark:text-gray-500"
                                                                >
                                                                    Slots:{" "}
                                                                    {cluster.slotRanges.length > 2
                                                                        ? `${cluster.slotRanges.slice(0, 2).join(", ")}, ...`
                                                                        : cluster.slotRanges.join(
                                                                              ", "
                                                                          )}
                                                                </Typography>
                                                            )}
                                                        </Link>
                                                    </div>

                                                    <div className="mt-3 flex space-x-2 overflow-x-auto sm:ml-6 sm:mt-0 md:hidden lg:flex xl:hidden 2xl:flex">
                                                        <Chip
                                                            icon={<DnsIcon fontSize="small" />}
                                                            label={`${cluster.shardCount} shards`}
                                                            size="small"
                                                            color="secondary"
                                                            variant="outlined"
                                                            className="whitespace-nowrap"
                                                        />

                                                        <Chip
                                                            icon={
                                                                <DeviceHubIcon fontSize="small" />
                                                            }
                                                            label={`${cluster.nodeCount} nodes`}
                                                            size="small"
                                                            color="default"
                                                            variant="outlined"
                                                            className="whitespace-nowrap"
                                                        />

                                                        {!cluster.hasSlots && (
                                                            <Chip
                                                                icon={<InfoIcon fontSize="small" />}
                                                                label="No slots"
                                                                size="small"
                                                                color="info"
                                                                variant="outlined"
                                                                className="whitespace-nowrap"
                                                            />
                                                        )}

                                                        {cluster.targetShardIndex >= 0 && (
                                                            <Chip
                                                                label={`Target shard: ${cluster.targetShardIndex + 1}`}
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
                                                                Shards
                                                            </Typography>
                                                            <Typography
                                                                variant="subtitle1"
                                                                className="font-semibold text-gray-900 dark:text-white"
                                                            >
                                                                {cluster.shardCount}
                                                            </Typography>
                                                        </div>

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
                                                                {cluster.nodeCount}
                                                            </Typography>
                                                        </div>
                                                    </div>

                                                    <div className="ml-2 mt-3 flex items-center space-x-2 sm:mt-0">
                                                        <Link
                                                            href={`/namespaces/${params.namespace}/clusters/${cluster.name}`}
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
                                            ? "No matching clusters"
                                            : "No clusters found"
                                    }
                                    description={
                                        filterOption !== "all"
                                            ? "Try changing your filter settings"
                                            : searchTerm
                                              ? "Try adjusting your search term"
                                              : "Create a cluster to get started"
                                    }
                                    icon={<StorageIcon sx={{ fontSize: 64 }} />}
                                    action={{
                                        label: "Create Cluster",
                                        onClick: () => {},
                                    }}
                                />
                                <div className="hidden">
                                    <ClusterCreation position="card" namespace={params.namespace} />
                                </div>
                            </div>
                        )}

                        {filteredAndSortedClusters.length > 0 && (
                            <div className="bg-gray-50 px-6 py-4 dark:bg-gray-800/30 sm:px-8">
                                <Typography
                                    variant="body2"
                                    className="text-gray-500 dark:text-gray-400"
                                >
                                    Showing {filteredAndSortedClusters.length} of{" "}
                                    {clusterData.length} clusters
                                </Typography>
                            </div>
                        )}
                    </Paper>
                </Box>
            </div>
        </div>
    );
}
