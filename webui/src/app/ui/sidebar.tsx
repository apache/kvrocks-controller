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

import { Divider, List, Typography, Paper, Box, Collapse } from "@mui/material";
import { fetchClusters, fetchNamespaces, listNodes, listShards } from "@/app/lib/api";
import Item from "./sidebarItem";
import { ClusterCreation, NamespaceCreation, NodeCreation, ShardCreation } from "./formCreation";
import Link from "next/link";
import { useState, useEffect } from "react";
import ChevronRightIcon from "@mui/icons-material/ChevronRight";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import FolderIcon from "@mui/icons-material/Folder";
import StorageIcon from "@mui/icons-material/Storage";
import DnsIcon from "@mui/icons-material/Dns";
import DeviceHubIcon from "@mui/icons-material/DeviceHub";

// Sidebar section header component
const SidebarHeader = ({
    title,
    count,
    isOpen,
    toggleOpen,
    icon,
}: {
    title: string;
    count: number;
    isOpen: boolean;
    toggleOpen: () => void;
    icon: React.ReactNode;
}) => (
    <div
        className="mb-0 flex cursor-pointer items-center justify-between rounded-xl bg-gray-50/80 px-4 py-2 shadow-sm transition-all duration-200 hover:bg-gray-100 hover:shadow-md dark:bg-dark-paper/80 dark:hover:bg-dark-border"
        onClick={toggleOpen}
    >
        <div className="flex items-center space-x-2">
            <div className="flex h-8 w-8 items-center justify-center rounded-full bg-primary/10 text-primary dark:bg-primary-dark/20 dark:text-primary-light">
                {icon}
            </div>
            <Typography
                variant="subtitle1"
                className="font-medium text-gray-800 dark:text-gray-200"
            >
                {title}
            </Typography>
            {count > 0 && (
                <span className="flex h-5 min-w-5 items-center justify-center rounded-full bg-primary/90 px-1.5 text-xs font-medium text-white shadow-sm dark:bg-primary">
                    {count}
                </span>
            )}
        </div>
        <div className="flex h-7 w-7 items-center justify-center rounded-full bg-white/80 text-gray-500 transition-transform dark:bg-dark-border/60 dark:text-gray-400">
            {isOpen ? (
                <ExpandMoreIcon fontSize="small" className="transform transition-transform" />
            ) : (
                <ChevronRightIcon fontSize="small" className="transform transition-transform" />
            )}
        </div>
    </div>
);

export function NamespaceSidebar() {
    const [namespaces, setNamespaces] = useState<string[]>([]);
    const [error, setError] = useState<string | null>(null);
    const [isOpen, setIsOpen] = useState(true);
    const [sidebarWidth, setSidebarWidth] = useState(260);
    const [isMobile, setIsMobile] = useState(false);

    useEffect(() => {
        const checkMobile = () => {
            setIsMobile(window.innerWidth < 768);
        };

        checkMobile();
        window.addEventListener("resize", checkMobile);
        return () => window.removeEventListener("resize", checkMobile);
    }, []);

    useEffect(() => {
        const fetchData = async () => {
            try {
                const fetchedNamespaces = await fetchNamespaces();
                setNamespaces(fetchedNamespaces);
            } catch (err) {
                setError("Failed to fetch namespaces");
            }
        };
        fetchData();
    }, []);

    const toggleSidebar = () => {
        if (isMobile) {
            setSidebarWidth(isOpen ? 0 : 260);
        }
        setIsOpen(!isOpen);
    };

    return (
        <Paper
            className="sidebar-container flex h-full flex-col overflow-hidden border-r border-light-border/50 bg-white/90 backdrop-blur-sm transition-all duration-300 dark:border-dark-border/50 dark:bg-dark-paper/90"
            elevation={0}
            sx={{
                width: `${sidebarWidth}px`,
                minWidth: isMobile ? 0 : "260px",
                maxWidth: "260px",
                borderTopRightRadius: "16px",
                borderBottomRightRadius: "16px",
                boxShadow: "4px 0 15px rgba(0, 0, 0, 0.03)",
                transition: "width 0.3s cubic-bezier(0.4, 0, 0.2, 1)",
            }}
        >
            {isMobile && (
                <button
                    onClick={toggleSidebar}
                    className="sidebar-toggle-btn absolute -right-10 top-4 z-50 flex h-9 w-9 items-center justify-center rounded-full bg-white text-gray-600 shadow-lg transition-all hover:bg-gray-50 dark:bg-dark-paper dark:text-gray-300 dark:hover:bg-dark-border"
                >
                    {isOpen ? (
                        <ChevronRightIcon />
                    ) : (
                        <ChevronRightIcon sx={{ transform: "rotate(180deg)" }} />
                    )}
                </button>
            )}

            <div className="sidebar-inner w-[260px]">
                <Box className="p-4 pb-2">
                    <NamespaceCreation position="sidebar" />
                </Box>

                <Box className="px-4 py-2">
                    <SidebarHeader
                        title="Namespaces"
                        count={namespaces.length}
                        isOpen={isOpen}
                        toggleOpen={toggleSidebar}
                        icon={<FolderIcon fontSize="small" />}
                    />
                </Box>

                <Collapse in={isOpen} className="flex-1 overflow-hidden">
                    <div className="h-full overflow-hidden px-4">
                        <div className="sidebar-scrollbar max-h-[calc(100vh-200px)] overflow-y-auto rounded-xl bg-gray-50/50 p-2 dark:bg-dark-border/20">
                            {error && (
                                <div className="my-2 rounded-lg bg-red-50 p-2 text-center text-sm text-red-600 dark:bg-red-900/20 dark:text-red-400">
                                    {error}
                                </div>
                            )}
                            <List className="p-0">
                                {namespaces.map((namespace) => (
                                    <Link
                                        href={`/namespaces/${namespace}`}
                                        passHref
                                        key={namespace}
                                    >
                                        <Item type="namespace" item={namespace} />
                                    </Link>
                                ))}
                            </List>
                        </div>
                    </div>
                </Collapse>
            </div>
        </Paper>
    );
}

export function ClusterSidebar({ namespace }: { namespace: string }) {
    const [clusters, setClusters] = useState<string[]>([]);
    const [error, setError] = useState<string | null>(null);
    const [isOpen, setIsOpen] = useState(true);
    const [sidebarWidth, setSidebarWidth] = useState(260);
    const [isMobile, setIsMobile] = useState(false);

    useEffect(() => {
        const checkMobile = () => {
            setIsMobile(window.innerWidth < 768);
        };

        checkMobile();
        window.addEventListener("resize", checkMobile);
        return () => window.removeEventListener("resize", checkMobile);
    }, []);

    useEffect(() => {
        const fetchData = async () => {
            try {
                const fetchedClusters = await fetchClusters(namespace);
                setClusters(fetchedClusters);
            } catch (err) {
                setError("Failed to fetch clusters");
            }
        };
        fetchData();
    }, [namespace]);

    const toggleSidebar = () => {
        if (isMobile) {
            setSidebarWidth(isOpen ? 0 : 260);
        }
        setIsOpen(!isOpen);
    };

    return (
        <Paper
            className="sidebar-container flex h-full flex-col overflow-hidden border-r border-light-border/50 bg-white/90 backdrop-blur-sm transition-all duration-300 dark:border-dark-border/50 dark:bg-dark-paper/90"
            elevation={0}
            sx={{
                width: `${sidebarWidth}px`,
                minWidth: isMobile ? 0 : "260px",
                maxWidth: "260px",
                borderTopRightRadius: "16px",
                borderBottomRightRadius: "16px",
                boxShadow: "4px 0 15px rgba(0, 0, 0, 0.03)",
                transition: "width 0.3s cubic-bezier(0.4, 0, 0.2, 1)",
            }}
        >
            {isMobile && (
                <button
                    onClick={toggleSidebar}
                    className="sidebar-toggle-btn absolute -right-10 top-4 z-50 flex h-9 w-9 items-center justify-center rounded-full bg-white text-gray-600 shadow-lg transition-all hover:bg-gray-50 dark:bg-dark-paper dark:text-gray-300 dark:hover:bg-dark-border"
                >
                    {isOpen ? (
                        <ChevronRightIcon />
                    ) : (
                        <ChevronRightIcon sx={{ transform: "rotate(180deg)" }} />
                    )}
                </button>
            )}

            <div className="sidebar-inner w-[260px]">
                <Box className="p-4 pb-2">
                    <ClusterCreation namespace={namespace} position="sidebar" />
                </Box>

                <Box className="px-4 py-2">
                    <SidebarHeader
                        title="Clusters"
                        count={clusters.length}
                        isOpen={isOpen}
                        toggleOpen={toggleSidebar}
                        icon={<StorageIcon fontSize="small" />}
                    />
                </Box>

                <Collapse in={isOpen} className="flex-1 overflow-hidden">
                    <div className="h-full overflow-hidden px-4">
                        <div className="sidebar-scrollbar max-h-[calc(100vh-200px)] overflow-y-auto rounded-xl bg-gray-50/50 p-2 dark:bg-dark-border/20">
                            {error && (
                                <div className="my-2 rounded-lg bg-red-50 p-2 text-center text-sm text-red-600 dark:bg-red-900/20 dark:text-red-400">
                                    {error}
                                </div>
                            )}
                            <List className="p-0">
                                {clusters.map((cluster) => (
                                    <Link
                                        href={`/namespaces/${namespace}/clusters/${cluster}`}
                                        passHref
                                        key={cluster}
                                    >
                                        <Item type="cluster" item={cluster} namespace={namespace} />
                                    </Link>
                                ))}
                            </List>
                        </div>
                    </div>
                </Collapse>
            </div>
        </Paper>
    );
}

export function ShardSidebar({ namespace, cluster }: { namespace: string; cluster: string }) {
    const [shards, setShards] = useState<string[]>([]);
    const [error, setError] = useState<string | null>(null);
    const [isOpen, setIsOpen] = useState(true);
    const [sidebarWidth, setSidebarWidth] = useState(260);
    const [isMobile, setIsMobile] = useState(false);

    useEffect(() => {
        const checkMobile = () => {
            setIsMobile(window.innerWidth < 768);
        };
        checkMobile();
        window.addEventListener("resize", checkMobile);
        return () => window.removeEventListener("resize", checkMobile);
    }, []);

    useEffect(() => {
        const fetchData = async () => {
            try {
                const fetchedShards = await listShards(namespace, cluster);
                const shardsIndex = fetchedShards.map(
                    (shard, index) => "Shard\t" + (index + 1).toString()
                );
                setShards(shardsIndex);
            } catch (err) {
                setError("Failed to fetch shards");
            }
        };
        fetchData();
    }, [namespace, cluster]);

    const toggleSidebar = () => {
        if (isMobile) {
            setSidebarWidth(isOpen ? 0 : 260);
        }
        setIsOpen(!isOpen);
    };

    return (
        <Paper
            className="sidebar-container flex h-full flex-col overflow-hidden border-r border-light-border/50 bg-white/90 backdrop-blur-sm transition-all duration-300 dark:border-dark-border/50 dark:bg-dark-paper/90"
            elevation={0}
            sx={{
                width: `${sidebarWidth}px`,
                minWidth: isMobile ? 0 : "260px",
                maxWidth: "260px",
                borderTopRightRadius: "16px",
                borderBottomRightRadius: "16px",
                boxShadow: "4px 0 15px rgba(0, 0, 0, 0.03)",
                transition: "width 0.3s cubic-bezier(0.4, 0, 0.2, 1)",
            }}
        >
            {isMobile && (
                <button
                    onClick={toggleSidebar}
                    className="sidebar-toggle-btn absolute -right-10 top-4 z-50 flex h-9 w-9 items-center justify-center rounded-full bg-white text-gray-600 shadow-lg transition-all hover:bg-gray-50 dark:bg-dark-paper dark:text-gray-300 dark:hover:bg-dark-border"
                >
                    {isOpen ? (
                        <ChevronRightIcon />
                    ) : (
                        <ChevronRightIcon sx={{ transform: "rotate(180deg)" }} />
                    )}
                </button>
            )}

            <div className="sidebar-inner w-[260px]">
                <Box className="p-4 pb-2">
                    <ShardCreation namespace={namespace} cluster={cluster} position="sidebar" />
                </Box>

                <Box className="px-4 py-2">
                    <SidebarHeader
                        title="Shards"
                        count={shards.length}
                        isOpen={isOpen}
                        toggleOpen={toggleSidebar}
                        icon={<DnsIcon fontSize="small" />}
                    />
                </Box>

                <Collapse in={isOpen} className="flex-1 overflow-hidden">
                    <div className="h-full overflow-hidden px-4">
                        <div className="custom-scrollbar max-h-[calc(100vh-180px)] overflow-y-auto rounded-xl bg-gray-50/50 p-2 dark:bg-dark-border/20">
                            {error && (
                                <div className="my-2 rounded-lg bg-red-50 p-2 text-center text-sm text-red-600 dark:bg-red-900/20 dark:text-red-400">
                                    {error}
                                </div>
                            )}
                            <List className="p-0">
                                {shards.map((shard, index) => (
                                    <Link
                                        href={`/namespaces/${namespace}/clusters/${cluster}/shards/${index}`}
                                        passHref
                                        key={index}
                                    >
                                        <Item
                                            type="shard"
                                            item={shard}
                                            namespace={namespace}
                                            cluster={cluster}
                                        />
                                    </Link>
                                ))}
                            </List>
                        </div>
                    </div>
                </Collapse>
            </div>
        </Paper>
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
    const [isOpen, setIsOpen] = useState(true);

    useEffect(() => {
        const fetchData = async () => {
            try {
                const fetchedNodes = (await listNodes(namespace, cluster, shard)) as NodeItem[];
                setNodes(fetchedNodes);
            } catch (err) {
                setError("Failed to fetch nodes");
            }
        };
        fetchData();
    }, [namespace, cluster, shard]);

    return (
        <Paper
            className="flex h-full w-64 flex-col overflow-hidden border-r border-light-border/50 bg-white/90 backdrop-blur-sm dark:border-dark-border/50 dark:bg-dark-paper/90"
            elevation={0}
            sx={{
                borderTopRightRadius: "16px",
                borderBottomRightRadius: "16px",
                boxShadow: "4px 0 15px rgba(0, 0, 0, 0.03)",
            }}
        >
            <Box className="p-4 pb-2">
                <NodeCreation
                    namespace={namespace}
                    cluster={cluster}
                    shard={shard}
                    position="sidebar"
                />
            </Box>

            <Box className="px-4 py-2">
                <SidebarHeader
                    title="Nodes"
                    count={nodes.length}
                    isOpen={isOpen}
                    toggleOpen={() => setIsOpen(!isOpen)}
                    icon={<DeviceHubIcon fontSize="small" />}
                />
            </Box>

            <Collapse in={isOpen} className="flex-1 overflow-hidden">
                <div className="h-full overflow-hidden px-4">
                    <div className="custom-scrollbar max-h-[calc(100vh-180px)] overflow-y-auto rounded-xl bg-gray-50/50 p-2 dark:bg-dark-border/20">
                        {error && (
                            <div className="my-2 rounded-lg bg-red-50 p-2 text-center text-sm text-red-600 dark:bg-red-900/20 dark:text-red-400">
                                {error}
                            </div>
                        )}
                        <List className="p-0">
                            {nodes.map((node, index) => (
                                <Link
                                    href={`/namespaces/${namespace}/clusters/${cluster}/shards/${shard}/nodes/${index}`}
                                    passHref
                                    key={index}
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
                            ))}
                        </List>
                    </div>
                </div>
            </Collapse>
        </Paper>
    );
}
