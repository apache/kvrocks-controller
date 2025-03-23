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

import { Box, Typography, Chip, Badge } from "@mui/material";
import { ShardSidebar } from "@/app/ui/sidebar";
import { fetchShard } from "@/app/lib/api";
import { useRouter } from "next/navigation";
import { useState, useEffect } from "react";
import { AddNodeCard, ResourceCard } from "@/app/ui/createCard";
import Link from "next/link";
import { LoadingSpinner } from "@/app/ui/loadingSpinner";
import { truncateText } from "@/app/utils";
import DeviceHubIcon from "@mui/icons-material/DeviceHub";
import DnsIcon from "@mui/icons-material/Dns";
import EmptyState from "@/app/ui/emptyState";
import AlarmIcon from "@mui/icons-material/Alarm";
import CheckCircleIcon from "@mui/icons-material/CheckCircle";
import RemoveCircleIcon from "@mui/icons-material/RemoveCircle";

export default function Shard({
    params,
}: {
    params: { namespace: string; cluster: string; shard: string };
}) {
    const { namespace, cluster, shard } = params;
    const [nodesData, setNodesData] = useState<any>(null);
    const [loading, setLoading] = useState<boolean>(true);
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

    return (
        <div className="flex h-full">
            <ShardSidebar namespace={namespace} cluster={cluster} />
            <div className="flex-1 overflow-auto">
                <Box className="container-inner">
                    <Box className="mb-6 flex items-center justify-between">
                        <div>
                            <Typography
                                variant="h5"
                                className="flex items-center font-medium text-gray-800 dark:text-gray-100"
                            >
                                <DnsIcon className="mr-2 text-primary dark:text-primary-light" />
                                Shard {parseInt(shard) + 1}
                                {nodesData?.nodes && (
                                    <Chip
                                        label={`${nodesData.nodes.length} nodes`}
                                        size="small"
                                        color="secondary"
                                        className="ml-3"
                                    />
                                )}
                            </Typography>
                            <Typography
                                variant="body2"
                                className="mt-1 text-gray-500 dark:text-gray-400"
                            >
                                {cluster} cluster in namespace {namespace}
                            </Typography>
                        </div>
                    </Box>

                    <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
                        <Box className="col-span-1">
                            <AddNodeCard namespace={namespace} cluster={cluster} shard={shard} />
                        </Box>

                        {nodesData?.nodes && nodesData.nodes.length > 0 ? (
                            nodesData.nodes.map((node: any, index: number) => {
                                const roleInfo = getRoleInfo(node.role);
                                return (
                                    <Link
                                        href={`/namespaces/${namespace}/clusters/${cluster}/shards/${shard}/nodes/${index}`}
                                        key={index}
                                        className="col-span-1"
                                    >
                                        <ResourceCard
                                            title={`Node ${index + 1}`}
                                            tags={[
                                                { label: node.role, color: roleInfo.color as any },
                                            ]}
                                        >
                                            <div className="mt-2 space-y-2 text-sm">
                                                <div className="flex items-center justify-between">
                                                    <span className="text-gray-500 dark:text-gray-400">
                                                        ID:
                                                    </span>
                                                    <span
                                                        className="max-w-[120px] overflow-hidden text-ellipsis rounded bg-gray-100 px-2 py-0.5 font-mono text-xs dark:bg-dark-border"
                                                        title={node.id}
                                                    >
                                                        {truncateText(node.id, 10)}
                                                    </span>
                                                </div>

                                                <div className="flex justify-between">
                                                    <span className="text-gray-500 dark:text-gray-400">
                                                        Address:
                                                    </span>
                                                    <span className="font-medium">{node.addr}</span>
                                                </div>

                                                <div className="flex items-center justify-between">
                                                    <span className="text-gray-500 dark:text-gray-400">
                                                        Uptime:
                                                    </span>
                                                    <span className="flex items-center">
                                                        <AlarmIcon
                                                            fontSize="small"
                                                            className="mr-1 text-gray-400 dark:text-gray-500"
                                                        />
                                                        {calculateUptime(node.created_at)}
                                                    </span>
                                                </div>
                                            </div>
                                        </ResourceCard>
                                    </Link>
                                );
                            })
                        ) : (
                            <Box className="col-span-full">
                                <EmptyState
                                    title="No nodes found"
                                    description="Create a node to get started"
                                    icon={<DeviceHubIcon sx={{ fontSize: 60 }} />}
                                />
                            </Box>
                        )}
                    </div>
                </Box>
            </div>
        </div>
    );
}
