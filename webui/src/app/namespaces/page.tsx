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
    Container,
    Typography,
    Paper,
    Box,
    Tooltip,
    Chip,
    Divider,
    Grid,
    Button,
    Card,
    CardContent,
    IconButton,
} from "@mui/material";
import { NamespaceSidebar } from "../ui/sidebar";
import { useRouter } from "next/navigation";
import { useState, useEffect, useRef } from "react";
import { deleteNamespace, fetchClusters, fetchNamespaces, listShards, listNodes } from "../lib/api";
import { LoadingSpinner } from "../ui/loadingSpinner";
import { NamespaceCreation } from "../ui/formCreation";
import Link from "next/link";
import FolderIcon from "@mui/icons-material/Folder";
import FolderOpenIcon from "@mui/icons-material/FolderOpen";
import EmptyState from "../ui/emptyState";
import StorageIcon from "@mui/icons-material/Storage";
import ChevronRightIcon from "@mui/icons-material/ChevronRight";
import AccessTimeIcon from "@mui/icons-material/AccessTime";
import DeleteIcon from "@mui/icons-material/Delete";
import InfoIcon from "@mui/icons-material/Info";
import DnsIcon from "@mui/icons-material/Dns";
import DeviceHubIcon from "@mui/icons-material/DeviceHub";
import EqualizerIcon from "@mui/icons-material/Equalizer";
import AddIcon from "@mui/icons-material/Add";
import SearchIcon from "@mui/icons-material/Search";
import SortIcon from "@mui/icons-material/Sort";
import FilterListIcon from "@mui/icons-material/FilterList";
import MoreVertIcon from "@mui/icons-material/MoreVert";
import ArrowUpwardIcon from "@mui/icons-material/ArrowUpward";
import ArrowDownwardIcon from "@mui/icons-material/ArrowDownward";
import CheckIcon from "@mui/icons-material/Check";
import Breadcrumb from "../ui/breadcrumb";
import { Popover, RadioGroup, FormControlLabel, Radio, Fade } from "@mui/material";

interface ResourceCounts {
    namespaces: number;
    clusters: number;
    shards: number;
    nodes: number;
}

interface NamespaceData {
    name: string;
    clusterCount: number;
    shardCount: number;
    nodeCount: number;
}

type FilterOption = "all" | "with-clusters" | "no-clusters";
type SortOption =
    | "name-asc"
    | "name-desc"
    | "clusters-desc"
    | "clusters-asc"
    | "nodes-desc"
    | "nodes-asc";

export default function Namespace() {
    const [namespacesData, setNamespacesData] = useState<NamespaceData[]>([]);
    const [resourceCounts, setResourceCounts] = useState<ResourceCounts>({
        namespaces: 0,
        clusters: 0,
        shards: 0,
        nodes: 0,
    });
    const [loading, setLoading] = useState<boolean>(true);
    const [deletingNamespace, setDeletingNamespace] = useState<string | null>(null);
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

                let totalClusters = 0;
                let totalShards = 0;
                let totalNodes = 0;
                const nsData: NamespaceData[] = [];

                for (const namespace of fetchedNamespaces) {
                    const clusters = await fetchClusters(namespace);
                    totalClusters += clusters.length;

                    let namespaceShardCount = 0;
                    let namespaceNodeCount = 0;

                    for (const cluster of clusters) {
                        const shards = await listShards(namespace, cluster);
                        if (Array.isArray(shards)) {
                            namespaceShardCount += shards.length;
                            totalShards += shards.length;

                            for (let i = 0; i < shards.length; i++) {
                                const nodes = await listNodes(namespace, cluster, i.toString());
                                if (Array.isArray(nodes)) {
                                    namespaceNodeCount += nodes.length;
                                    totalNodes += nodes.length;
                                }
                            }
                        }
                    }

                    nsData.push({
                        name: namespace,
                        clusterCount: clusters.length,
                        shardCount: namespaceShardCount,
                        nodeCount: namespaceNodeCount,
                    });
                }

                setNamespacesData(nsData);
                setResourceCounts({
                    namespaces: fetchedNamespaces.length,
                    clusters: totalClusters,
                    shards: totalShards,
                    nodes: totalNodes,
                });
            } catch (error) {
                console.error("Error fetching namespaces data:", error);
            } finally {
                setLoading(false);
            }
        };

        fetchData();
    }, [router]);

    const handleDeleteNamespace = async (namespace: string) => {
        if (confirm(`Are you sure you want to delete namespace "${namespace}"?`)) {
            try {
                setDeletingNamespace(namespace);
                await deleteNamespace(namespace);
                setNamespacesData(namespacesData.filter((ns) => ns.name !== namespace));
                setResourceCounts((prev) => ({
                    ...prev,
                    namespaces: prev.namespaces - 1,
                }));
            } catch (error) {
                console.error(`Error deleting namespace ${namespace}:`, error);
                alert(`Failed to delete namespace: ${error}`);
            } finally {
                setDeletingNamespace(null);
            }
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

    const filteredAndSortedNamespaces = namespacesData
        .filter((ns) => {
            if (!ns.name.toLowerCase().includes(searchTerm.toLowerCase())) {
                return false;
            }

            switch (filterOption) {
                case "with-clusters":
                    return ns.clusterCount > 0;
                case "no-clusters":
                    return ns.clusterCount === 0;
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
                case "clusters-desc":
                    return b.clusterCount - a.clusterCount;
                case "clusters-asc":
                    return a.clusterCount - b.clusterCount;
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
                                className="font-medium text-gray-900 dark:text-white"
                            >
                                Namespaces
                            </Typography>
                            <Typography
                                variant="body1"
                                className="mt-0.5 text-gray-500 dark:text-gray-400"
                            >
                                Manage your Kvrocks database namespaces
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
                                        placeholder="Search namespaces..."
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

                            <div className="flex-shrink-0">
                                <NamespaceCreation position="page">
                                    <Button
                                        variant="contained"
                                        color="primary"
                                        className="whitespace-nowrap rounded-lg px-5 py-2.5 font-medium shadow-md transition-all hover:shadow-lg"
                                        startIcon={<AddIcon />}
                                        disableElevation
                                        size="medium"
                                    >
                                        Create Namespace
                                    </Button>
                                </NamespaceCreation>
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
                                        <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-blue-50 text-blue-500 dark:bg-blue-900/30 dark:text-blue-400">
                                            <FolderIcon sx={{ fontSize: 24 }} />
                                        </div>
                                        <div className="flex flex-col items-end">
                                            <Typography
                                                variant="h4"
                                                className="font-semibold text-gray-900 dark:text-white"
                                            >
                                                {resourceCounts.namespaces}
                                            </Typography>
                                            <Typography
                                                variant="body2"
                                                className="text-gray-500 dark:text-gray-400"
                                            >
                                                Namespaces
                                            </Typography>
                                        </div>
                                    </div>
                                    <div className="absolute -bottom-4 -right-4 h-24 w-24 rounded-full bg-blue-500/5 blur-xl"></div>
                                </Paper>
                            </Grid>

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
                                    All Namespaces
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
                                        Filter Namespaces
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
                                                            All namespaces
                                                        </span>
                                                        <Chip
                                                            size="small"
                                                            label={namespacesData.length}
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
                                                value="with-clusters"
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
                                                            With clusters
                                                        </span>
                                                        <Chip
                                                            size="small"
                                                            label={
                                                                namespacesData.filter(
                                                                    (ns) => ns.clusterCount > 0
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
                                                value="no-clusters"
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
                                                            No clusters
                                                        </span>
                                                        <Chip
                                                            size="small"
                                                            label={
                                                                namespacesData.filter(
                                                                    (ns) => ns.clusterCount === 0
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
                                        Sort By
                                    </Typography>
                                </div>

                                <RadioGroup
                                    value={sortOption}
                                    onChange={(e) => setSortOption(e.target.value as SortOption)}
                                >
                                    <div className="space-y-2">
                                        <div className="mb-2 text-xs font-medium uppercase text-gray-500 dark:text-gray-400">
                                            Name
                                        </div>
                                        <div className="space-y-1">
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
                                                                A to Z
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
                                                                Z to A
                                                            </span>
                                                        </div>
                                                    }
                                                    className="m-0 w-full"
                                                />
                                            </div>
                                        </div>

                                        <div className="mb-2 mt-4 text-xs font-medium uppercase text-gray-500 dark:text-gray-400">
                                            Clusters
                                        </div>
                                        <div className="space-y-1">
                                            <div className="rounded-lg bg-gray-50 p-2 dark:bg-gray-800">
                                                <FormControlLabel
                                                    value="clusters-desc"
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
                                                                Most clusters
                                                            </span>
                                                        </div>
                                                    }
                                                    className="m-0 w-full"
                                                />
                                            </div>
                                            <div className="rounded-lg bg-gray-50 p-2 dark:bg-gray-800">
                                                <FormControlLabel
                                                    value="clusters-asc"
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
                                                                Fewest clusters
                                                            </span>
                                                        </div>
                                                    }
                                                    className="m-0 w-full"
                                                />
                                            </div>
                                        </div>

                                        <div className="mb-2 mt-4 text-xs font-medium uppercase text-gray-500 dark:text-gray-400">
                                            Nodes
                                        </div>
                                        <div className="space-y-1">
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
                                                                Fewest nodes
                                                            </span>
                                                        </div>
                                                    }
                                                    className="m-0 w-full"
                                                />
                                            </div>
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

                        {filteredAndSortedNamespaces.length > 0 ? (
                            <div className="divide-y divide-gray-100 dark:divide-gray-800">
                                {filteredAndSortedNamespaces.map((nsData) => (
                                    <div
                                        key={nsData.name}
                                        className="group p-2 transition-colors hover:bg-gray-50 dark:hover:bg-gray-800/30"
                                    >
                                        <Paper
                                            elevation={0}
                                            className="overflow-hidden rounded-xl border border-transparent bg-white p-4 transition-all group-hover:border-primary/10 group-hover:shadow-sm dark:bg-dark-paper dark:group-hover:border-primary-dark/20"
                                        >
                                            <div className="flex flex-col items-start sm:flex-row sm:items-center">
                                                <div className="mb-3 flex h-14 w-14 flex-shrink-0 items-center justify-center rounded-xl bg-blue-50 text-blue-500 dark:bg-blue-900/30 dark:text-blue-400 sm:mb-0">
                                                    <FolderIcon sx={{ fontSize: 28 }} />
                                                </div>

                                                <div className="flex flex-1 flex-col sm:ml-5 sm:flex-row sm:items-center sm:overflow-hidden">
                                                    <div className="flex-1 overflow-hidden">
                                                        <Link
                                                            href={`/namespaces/${nsData.name}`}
                                                            className="block"
                                                        >
                                                            <Typography
                                                                variant="h6"
                                                                className="truncate font-medium text-gray-900 transition-colors hover:text-primary dark:text-gray-100 dark:hover:text-primary-light"
                                                            >
                                                                {nsData.name}
                                                            </Typography>
                                                            <Typography
                                                                variant="body2"
                                                                className="flex items-center text-gray-500 dark:text-gray-400"
                                                            >
                                                                <AccessTimeIcon
                                                                    sx={{ fontSize: 14 }}
                                                                    className="mr-1"
                                                                />
                                                                Created recently
                                                            </Typography>
                                                        </Link>
                                                    </div>

                                                    <div className="mt-3 flex space-x-2 overflow-x-auto sm:ml-6 sm:mt-0 md:hidden lg:flex xl:hidden 2xl:flex">
                                                        <Chip
                                                            icon={<StorageIcon fontSize="small" />}
                                                            label={`${nsData.clusterCount} clusters`}
                                                            size="small"
                                                            color="primary"
                                                            variant="outlined"
                                                            className="whitespace-nowrap"
                                                        />

                                                        <Chip
                                                            icon={<DnsIcon fontSize="small" />}
                                                            label={`${nsData.shardCount} shards`}
                                                            size="small"
                                                            color="secondary"
                                                            variant="outlined"
                                                            className="whitespace-nowrap"
                                                        />

                                                        <Chip
                                                            icon={
                                                                <DeviceHubIcon fontSize="small" />
                                                            }
                                                            label={`${nsData.nodeCount} nodes`}
                                                            size="small"
                                                            color="default"
                                                            variant="outlined"
                                                            className="whitespace-nowrap"
                                                        />
                                                    </div>

                                                    <div className="hidden space-x-3 sm:ml-8 md:flex lg:hidden xl:flex 2xl:hidden">
                                                        <div className="flex flex-col items-center rounded-lg border border-gray-100 bg-gray-50 px-4 py-2 dark:border-gray-800 dark:bg-gray-800/50">
                                                            <Typography
                                                                variant="caption"
                                                                className="text-gray-500 dark:text-gray-400"
                                                            >
                                                                Clusters
                                                            </Typography>
                                                            <Typography
                                                                variant="subtitle1"
                                                                className="font-semibold text-gray-900 dark:text-white"
                                                            >
                                                                {nsData.clusterCount}
                                                            </Typography>
                                                        </div>

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
                                                                {nsData.shardCount}
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
                                                                {nsData.nodeCount}
                                                            </Typography>
                                                        </div>
                                                    </div>

                                                    <div className="ml-2 mt-3 flex items-center space-x-2 sm:mt-0">
                                                        <button
                                                            onClick={() =>
                                                                handleDeleteNamespace(nsData.name)
                                                            }
                                                            disabled={
                                                                deletingNamespace === nsData.name
                                                            }
                                                            className="rounded-full bg-gray-100 p-2 text-gray-600 transition-colors hover:bg-red-100 hover:text-red-600 disabled:cursor-not-allowed disabled:opacity-50 dark:bg-gray-800 dark:text-gray-300 dark:hover:bg-red-900/30 dark:hover:text-red-400"
                                                        >
                                                            <DeleteIcon />
                                                        </button>

                                                        <Link
                                                            href={`/namespaces/${nsData.name}`}
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
                                            ? "No matching namespaces"
                                            : "No namespaces found"
                                    }
                                    description={
                                        filterOption !== "all"
                                            ? "Try changing your filter settings"
                                            : searchTerm
                                              ? "Try adjusting your search term"
                                              : "Create a namespace to get started"
                                    }
                                    icon={
                                        <FolderOpenIcon
                                            sx={{ fontSize: 64 }}
                                            className="text-gray-300 dark:text-gray-600"
                                        />
                                    }
                                    action={{
                                        label: "Create Namespace",
                                        onClick: () =>
                                            document
                                                .getElementById("create-namespace-btn")
                                                ?.click(),
                                    }}
                                />
                                <div className="hidden">
                                    <NamespaceCreation position="page">
                                        <Button id="create-namespace-btn">Create</Button>
                                    </NamespaceCreation>
                                </div>
                            </div>
                        )}

                        {filteredAndSortedNamespaces.length > 0 && (
                            <div className="bg-gray-50 px-6 py-4 dark:bg-gray-800/30 sm:px-8">
                                <Typography
                                    variant="body2"
                                    className="text-gray-500 dark:text-gray-400"
                                >
                                    Showing {filteredAndSortedNamespaces.length} of{" "}
                                    {namespacesData.length} namespaces
                                    {filterOption !== "all" && " (filtered)"}
                                </Typography>
                            </div>
                        )}
                    </Paper>
                </Box>
            </div>
        </div>
    );
}
