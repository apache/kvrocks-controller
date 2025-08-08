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
import { ShardSidebar } from "@/app/ui/sidebar";
import { fetchShard } from "@/app/lib/api";
import { useRouter } from "next/navigation";
import { useState, useEffect } from "react";
import { AddNodeCard } from "@/app/ui/createCard";
import Link from "next/link";
import { LoadingSpinner } from "@/app/ui/loadingSpinner";
import { truncateText } from "@/app/utils";
import DeviceHubIcon from "@mui/icons-material/DeviceHub";
import DnsIcon from "@mui/icons-material/Dns";
import EmptyState from "@/app/ui/emptyState";
import AlarmIcon from "@mui/icons-material/Alarm";
import CheckCircleIcon from "@mui/icons-material/CheckCircle";
import RemoveCircleIcon from "@mui/icons-material/RemoveCircle";
import SearchIcon from "@mui/icons-material/Search";
import FilterListIcon from "@mui/icons-material/FilterList";
import SortIcon from "@mui/icons-material/Sort";
import CheckIcon from "@mui/icons-material/Check";
import ArrowUpwardIcon from "@mui/icons-material/ArrowUpward";
import ArrowDownwardIcon from "@mui/icons-material/ArrowDownward";
import { NodeCreation } from "@/app/ui/formCreation";
import AddIcon from "@mui/icons-material/Add";

export default function Shard({
    params,
}: {
    params: { namespace: string; cluster: string; shard: string };
}) {
    const { namespace, cluster, shard } = params;
    const [nodesData, setNodesData] = useState<any>(null);
    const [loading, setLoading] = useState<boolean>(true);
    const [searchTerm, setSearchTerm] = useState<string>("");
    const [filterAnchorEl, setFilterAnchorEl] = useState<null | HTMLElement>(null);
    const [sortAnchorEl, setSortAnchorEl] = useState<null | HTMLElement>(null);
    const [filterOption, setFilterOption] = useState<string>("all");
    const [sortOption, setSortOption] = useState<string>("index-asc");
    const router = useRouter();

    useEffect(() => {
        const fetchData = async () => {
            try {
                const fetchedNodes = await fetchShard(namespace, cluster, shard);
                if (!fetchedNodes) {
                    console.error(`Shard ${shard} not found`);
                    router.push("/404");
                    return;
                }
                setNodesData(fetchedNodes);
            } catch (error) {
                console.error("Error fetching shard data:", error);
            } finally {
                setLoading(false);
            }
        };
        fetchData();
    }, [namespace, cluster, shard, router]);

    if (loading) {
        return <LoadingSpinner />;
    }

    // Calculate uptime from creation timestamp
    const calculateUptime = (timestamp: number) => {
        const now = Math.floor(Date.now() / 1000);
        const uptimeSeconds = now - timestamp;
        if (uptimeSeconds < 60) return `${uptimeSeconds} seconds`;
        if (uptimeSeconds < 3600) return `${Math.floor(uptimeSeconds / 60)} minutes`;
        if (uptimeSeconds < 86400) return `${Math.floor(uptimeSeconds / 3600)} hours`;
        return `${Math.floor(uptimeSeconds / 86400)} days`;
    };

    // Get role color and icon
    const getRoleInfo = (role: string) => {
        if (role === "master") {
            return {
                color: "success",
                icon: <CheckCircleIcon fontSize="small" className="text-success" />,
            };
        }
        return {
            color: "info",
            icon: <DeviceHubIcon fontSize="small" className="text-info" />,
        };
    };

    // Filtering and sorting logic for nodes
    const filteredAndSortedNodes = (nodesData?.nodes || [])
        .filter((node: any, idx: number) => {
            if (!`node ${idx + 1}`.toLowerCase().includes(searchTerm.toLowerCase())) {
                return false;
            }
            switch (filterOption) {
                case "master":
                    return node.role === "master";
                case "replica":
                    return node.role !== "master";
                default:
                    return true;
            }
        })
        .sort((a: any, b: any) => {
            switch (sortOption) {
                case "index-asc":
                    return a.index - b.index;
                case "index-desc":
                    return b.index - a.index;
                case "uptime-desc":
                    return b.created_at - a.created_at;
                case "uptime-asc":
                    return a.created_at - b.created_at;
                default:
                    return 0;
            }
        });

    const isFilterOpen = Boolean(filterAnchorEl);
    const isSortOpen = Boolean(sortAnchorEl);
    const filterId = isFilterOpen ? "filter-popover" : undefined;
    const sortId = isSortOpen ? "sort-popover" : undefined;

    return (
        <div className="flex h-full">
            <ShardSidebar namespace={namespace} cluster={cluster} />
            <div className="no-scrollbar flex-1 overflow-y-auto bg-white pb-8 dark:bg-dark">
                <Box className="px-6 py-4 sm:px-8 sm:py-6">
                    <div className="mb-4 flex flex-col gap-3 sm:mb-5 lg:flex-row lg:items-center lg:justify-between">
                        <div>
                            <Typography
                                variant="h4"
                                className="flex items-center font-medium text-gray-900 dark:text-white"
                            >
                                <DnsIcon className="mr-3 text-primary dark:text-primary-light" />
                                Shard {parseInt(shard) + 1}
                            </Typography>
                            <Typography
                                variant="body1"
                                className="mt-0.5 text-gray-500 dark:text-gray-400"
                            >
                                Manage nodes in this shard
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
                                        placeholder="Search nodes..."
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
                                <NodeCreation
                                    position="card"
                                    namespace={namespace}
                                    cluster={cluster}
                                    shard={shard}
                                >
                                    <Button
                                        variant="outlined"
                                        color="primary"
                                        className="whitespace-nowrap px-5 py-2.5 font-medium shadow-sm transition-all hover:shadow-md"
                                        style={{ borderRadius: "16px" }}
                                        startIcon={<AddIcon />}
                                        disableElevation
                                        size="medium"
                                    >
                                        Create Node
                                    </Button>
                                </NodeCreation>
                            </div>
                        </div>
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
                                    All Nodes
                                </Typography>
                                <div className="flex items-center gap-2">
                                    <Tooltip title="Filter">
                                        <IconButton
                                            size="small"
                                            onClick={(e) => setFilterAnchorEl(e.currentTarget)}
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
                                            onClick={(e) => setSortAnchorEl(e.currentTarget)}
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
                            onClose={() => setFilterAnchorEl(null)}
                            anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
                            transformOrigin={{ vertical: "top", horizontal: "right" }}
                            TransitionComponent={Fade}
                            PaperProps={{
                                className: "shadow-xl border border-gray-100 dark:border-gray-700",
                                elevation: 3,
                                sx: { width: 220, borderRadius: "20px" },
                            }}
                        >
                            <div className="p-4">
                                <div className="mb-3 flex items-center justify-between border-b border-gray-100 pb-2 dark:border-gray-700">
                                    <Typography variant="subtitle1" className="font-medium">
                                        Filter Nodes
                                    </Typography>
                                </div>
                                <RadioGroup
                                    value={filterOption}
                                    onChange={(e) => setFilterOption(e.target.value)}
                                >
                                    <div className="space-y-2">
                                        <div
                                            className="bg-gray-50 p-2 dark:bg-gray-800"
                                            style={{ borderRadius: "12px" }}
                                        >
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
                                                    <span className="text-sm font-medium">
                                                        All nodes
                                                    </span>
                                                }
                                                className="m-0 w-full"
                                            />
                                        </div>
                                        <div
                                            className="bg-gray-50 p-2 dark:bg-gray-800"
                                            style={{ borderRadius: "12px" }}
                                        >
                                            <FormControlLabel
                                                value="master"
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
                                                    <span className="text-sm font-medium">
                                                        Master nodes
                                                    </span>
                                                }
                                                className="m-0 w-full"
                                            />
                                        </div>
                                        <div
                                            className="bg-gray-50 p-2 dark:bg-gray-800"
                                            style={{ borderRadius: "12px" }}
                                        >
                                            <FormControlLabel
                                                value="replica"
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
                                                    <span className="text-sm font-medium">
                                                        Replica nodes
                                                    </span>
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
                                        onClick={() => setFilterAnchorEl(null)}
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
                            onClose={() => setSortAnchorEl(null)}
                            anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
                            transformOrigin={{ vertical: "top", horizontal: "right" }}
                            TransitionComponent={Fade}
                            PaperProps={{
                                className: "shadow-xl border border-gray-100 dark:border-gray-700",
                                elevation: 3,
                                sx: { width: 220, borderRadius: "20px" },
                            }}
                        >
                            <div className="p-4">
                                <div className="mb-3 flex items-center justify-between border-b border-gray-100 pb-2 dark:border-gray-700">
                                    <Typography variant="subtitle1" className="font-medium">
                                        Sort Nodes
                                    </Typography>
                                </div>
                                <RadioGroup
                                    value={sortOption}
                                    onChange={(e) => setSortOption(e.target.value)}
                                >
                                    <div className="space-y-2">
                                        <div
                                            className="bg-gray-50 p-2 dark:bg-gray-800"
                                            style={{ borderRadius: "12px" }}
                                        >
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
                                                    <span className="text-sm font-medium">
                                                        Index 1-N
                                                    </span>
                                                }
                                                className="m-0 w-full"
                                            />
                                        </div>
                                        <div
                                            className="bg-gray-50 p-2 dark:bg-gray-800"
                                            style={{ borderRadius: "12px" }}
                                        >
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
                                                    <span className="text-sm font-medium">
                                                        Index N-1
                                                    </span>
                                                }
                                                className="m-0 w-full"
                                            />
                                        </div>
                                        <div
                                            className="bg-gray-50 p-2 dark:bg-gray-800"
                                            style={{ borderRadius: "12px" }}
                                        >
                                            <FormControlLabel
                                                value="uptime-desc"
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
                                                    <span className="text-sm font-medium">
                                                        Newest
                                                    </span>
                                                }
                                                className="m-0 w-full"
                                            />
                                        </div>
                                        <div
                                            className="bg-gray-50 p-2 dark:bg-gray-800"
                                            style={{ borderRadius: "12px" }}
                                        >
                                            <FormControlLabel
                                                value="uptime-asc"
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
                                                    <span className="text-sm font-medium">
                                                        Oldest
                                                    </span>
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
                                        onClick={() => setSortAnchorEl(null)}
                                        className="px-3 py-1 text-xs"
                                        style={{ borderRadius: "12px" }}
                                    >
                                        Close
                                    </Button>
                                </div>
                            </div>
                        </Popover>
                        {filteredAndSortedNodes.length > 0 ? (
                            <div className="divide-y divide-gray-100 dark:divide-gray-800">
                                {filteredAndSortedNodes.map((node: any, index: number) => {
                                    const roleInfo = getRoleInfo(node.role);
                                    return (
                                        <div
                                            key={index}
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
                                                        <DeviceHubIcon sx={{ fontSize: 28 }} />
                                                    </div>
                                                    <div className="flex flex-1 flex-col sm:ml-5 sm:flex-row sm:items-center sm:overflow-hidden">
                                                        <div className="flex-1 overflow-hidden">
                                                            <Link
                                                                href={`/namespaces/${namespace}/clusters/${cluster}/shards/${shard}/nodes/${index}`}
                                                                className="block"
                                                            >
                                                                <div className="flex items-center gap-2">
                                                                    <Typography
                                                                        variant="h6"
                                                                        className="truncate font-medium text-gray-900 transition-colors hover:text-primary dark:text-gray-100 dark:hover:text-primary-light"
                                                                    >
                                                                        Node {index + 1}
                                                                    </Typography>
                                                                    {node.role === "master" ? (
                                                                        <div
                                                                            className="flex items-center gap-1 border border-green-200 bg-green-50 px-2.5 py-1 dark:border-green-800 dark:bg-green-900/30"
                                                                            style={{
                                                                                borderRadius:
                                                                                    "12px",
                                                                            }}
                                                                        >
                                                                            <div className="h-1.5 w-1.5 rounded-full bg-green-500"></div>
                                                                            <span className="text-xs font-medium text-green-700 dark:text-green-300">
                                                                                Master
                                                                            </span>
                                                                        </div>
                                                                    ) : (
                                                                        <div
                                                                            className="flex items-center gap-1 border border-blue-200 bg-blue-50 px-2.5 py-1 dark:border-blue-800 dark:bg-blue-900/30"
                                                                            style={{
                                                                                borderRadius:
                                                                                    "12px",
                                                                            }}
                                                                        >
                                                                            <div className="h-1.5 w-1.5 rounded-full bg-blue-500"></div>
                                                                            <span className="text-xs font-medium text-blue-700 dark:text-blue-300">
                                                                                Replica
                                                                            </span>
                                                                        </div>
                                                                    )}
                                                                </div>
                                                                <div className="mt-2 space-y-1">
                                                                    <Typography
                                                                        variant="body2"
                                                                        className="flex items-center text-gray-500 dark:text-gray-400"
                                                                    >
                                                                        ID:{" "}
                                                                        <span className="ml-1 font-mono text-xs">
                                                                            {truncateText(
                                                                                node.id,
                                                                                10
                                                                            )}
                                                                        </span>
                                                                    </Typography>
                                                                    <Typography
                                                                        variant="body2"
                                                                        className="flex items-center text-gray-500 dark:text-gray-400"
                                                                    >
                                                                        Address:{" "}
                                                                        <span className="ml-1">
                                                                            {node.addr}
                                                                        </span>
                                                                    </Typography>
                                                                    <Typography
                                                                        variant="body2"
                                                                        className="flex items-center text-gray-500 dark:text-gray-400"
                                                                    >
                                                                        <AlarmIcon
                                                                            fontSize="small"
                                                                            className="mr-1 text-gray-400 dark:text-gray-500"
                                                                        />
                                                                        Uptime:{" "}
                                                                        {calculateUptime(
                                                                            node.created_at
                                                                        )}
                                                                    </Typography>
                                                                </div>
                                                            </Link>
                                                        </div>
                                                    </div>
                                                </div>
                                            </Paper>
                                        </div>
                                    );
                                })}
                            </div>
                        ) : (
                            <div className="p-12">
                                <EmptyState
                                    title={
                                        filterOption !== "all"
                                            ? "No matching nodes"
                                            : "No nodes found"
                                    }
                                    description={
                                        filterOption !== "all"
                                            ? "Try changing your filter settings"
                                            : searchTerm
                                              ? "Try adjusting your search term"
                                              : "Create a node to get started"
                                    }
                                    icon={<DeviceHubIcon sx={{ fontSize: 64 }} />}
                                />
                            </div>
                        )}
                        {filteredAndSortedNodes.length > 0 && (
                            <div className="bg-gray-50 px-6 py-4 dark:bg-gray-800/30 sm:px-8">
                                <Typography
                                    variant="body2"
                                    className="text-gray-500 dark:text-gray-400"
                                >
                                    Showing {filteredAndSortedNodes.length} of{" "}
                                    {nodesData.nodes.length} nodes
                                </Typography>
                            </div>
                        )}
                    </Paper>
                </Box>
            </div>
        </div>
    );
}
