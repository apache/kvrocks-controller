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
        className="mb-2 flex cursor-pointer items-center justify-between rounded-md bg-gray-50 px-4 py-3 transition-colors hover:bg-gray-100 dark:bg-dark-paper dark:hover:bg-dark-border"
        onClick={toggleOpen}
    >
        <div className="flex items-center space-x-2">
            {icon}
            <Typography variant="subtitle1" className="font-medium">
                {title}
            </Typography>
            {count > 0 && (
                <span className="rounded-full bg-primary px-2 py-0.5 text-xs text-white dark:bg-primary-dark">
                    {count}
                </span>
            )}
        </div>
        {isOpen ? <ExpandMoreIcon fontSize="small" /> : <ChevronRightIcon fontSize="small" />}
    </div>
);

export function NamespaceSidebar() {
    const [namespaces, setNamespaces] = useState<string[]>([]);
    const [error, setError] = useState<string | null>(null);
    const [isOpen, setIsOpen] = useState(true);

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

    return (
        <Paper
            className="flex h-full w-64 flex-col overflow-hidden border-r border-light-border dark:border-dark-border"
            elevation={0}
            square
        >
            <Box className="p-4">
                <NamespaceCreation position="sidebar" />
            </Box>

            <SidebarHeader
                title="Namespaces"
                count={namespaces.length}
                isOpen={isOpen}
                toggleOpen={() => setIsOpen(!isOpen)}
                icon={<FolderIcon className="text-primary dark:text-primary-light" />}
            />

            <Collapse in={isOpen}>
                <List className="max-h-[calc(100vh-180px)] overflow-y-auto px-2">
                    {error && (
                        <Typography color="error" align="center" className="py-2 text-sm">
                            {error}
                        </Typography>
                    )}
                    {namespaces.map((namespace) => (
                        <Link href={`/namespaces/${namespace}`} passHref key={namespace}>
                            <Item type="namespace" item={namespace} />
                        </Link>
                    ))}
                </List>
            </Collapse>
        </Paper>
    );
}

export function ClusterSidebar({ namespace }: { namespace: string }) {
    const [clusters, setClusters] = useState<string[]>([]);
    const [error, setError] = useState<string | null>(null);
    const [isOpen, setIsOpen] = useState(true);

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

    return (
        <Paper
            className="flex h-full w-64 flex-col overflow-hidden border-r border-light-border dark:border-dark-border"
            elevation={0}
            square
        >
            <Box className="p-4">
                <ClusterCreation namespace={namespace} position="sidebar" />
            </Box>

            <SidebarHeader
                title="Clusters"
                count={clusters.length}
                isOpen={isOpen}
                toggleOpen={() => setIsOpen(!isOpen)}
                icon={<StorageIcon className="text-primary dark:text-primary-light" />}
            />

            <Collapse in={isOpen}>
                <List className="max-h-[calc(100vh-180px)] overflow-y-auto px-2">
                    {error && (
                        <Typography color="error" align="center" className="py-2 text-sm">
                            {error}
                        </Typography>
                    )}
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
            </Collapse>
        </Paper>
    );
}

export function ShardSidebar({ namespace, cluster }: { namespace: string; cluster: string }) {
    const [shards, setShards] = useState<string[]>([]);
    const [error, setError] = useState<string | null>(null);
    const [isOpen, setIsOpen] = useState(true);

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

    return (
        <Paper
            className="flex h-full w-64 flex-col overflow-hidden border-r border-light-border dark:border-dark-border"
            elevation={0}
            square
        >
            <Box className="p-4">
                <ShardCreation namespace={namespace} cluster={cluster} position="sidebar" />
            </Box>

            <SidebarHeader
                title="Shards"
                count={shards.length}
                isOpen={isOpen}
                toggleOpen={() => setIsOpen(!isOpen)}
                icon={<DnsIcon className="text-primary dark:text-primary-light" />}
            />

            <Collapse in={isOpen}>
                <List className="max-h-[calc(100vh-180px)] overflow-y-auto px-2">
                    {error && (
                        <Typography color="error" align="center" className="py-2 text-sm">
                            {error}
                        </Typography>
                    )}
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
            </Collapse>
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
            className="flex h-full w-64 flex-col overflow-hidden border-r border-light-border dark:border-dark-border"
            elevation={0}
            square
        >
            <Box className="p-4">
                <NodeCreation
                    namespace={namespace}
                    cluster={cluster}
                    shard={shard}
                    position="sidebar"
                />
            </Box>

            <SidebarHeader
                title="Nodes"
                count={nodes.length}
                isOpen={isOpen}
                toggleOpen={() => setIsOpen(!isOpen)}
                icon={<DeviceHubIcon className="text-primary dark:text-primary-light" />}
            />

            <Collapse in={isOpen}>
                <List className="max-h-[calc(100vh-180px)] overflow-y-auto px-2">
                    {error && (
                        <Typography color="error" align="center" className="py-2 text-sm">
                            {error}
                        </Typography>
                    )}
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
            </Collapse>
        </Paper>
    );
}
