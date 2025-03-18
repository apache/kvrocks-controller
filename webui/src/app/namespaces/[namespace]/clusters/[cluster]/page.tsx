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
    Container,
    Typography,
    Chip,
    Badge,
} from "@mui/material";
import { ClusterSidebar } from "../../../../ui/sidebar";
import { useState, useEffect } from "react";
import { listShards } from "@/app/lib/api";
import { AddShardCard, ResourceCard } from "@/app/ui/createCard";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { LoadingSpinner } from "@/app/ui/loadingSpinner";
import DnsIcon from '@mui/icons-material/Dns';
import StorageIcon from '@mui/icons-material/Storage';
import EmptyState from "@/app/ui/emptyState";

export default function Cluster({
    params,
}: {
  params: { namespace: string; cluster: string };
}) {
    const { namespace, cluster } = params;
    const [shardsData, setShardsData] = useState<any[]>([]);
    const [loading, setLoading] = useState<boolean>(true);
    const router = useRouter();

    useEffect(() => {
        const fetchData = async () => {
            try {
                const fetchedShards = await listShards(namespace, cluster);

                if (!fetchedShards) {
                    console.error(`Shards not found`);
                    router.push("/404");
                    return;
                }

                setShardsData(fetchedShards);
            } catch (error) {
                console.error("Error fetching shards:", error);
            } finally {
                setLoading(false);
            }
        };
        fetchData();
    }, [namespace, cluster, router]);

    if (loading) {
        return <LoadingSpinner />;
    }

    const formatSlotRanges = (ranges: string[]) => {
        if (!ranges || ranges.length === 0) return "None";
        if (ranges.length <= 2) return ranges.join(", ");
        return `${ranges[0]}, ${ranges[1]}, ... (+${ranges.length - 2} more)`;
    };

    return (
        <div className="flex h-full">
            <ClusterSidebar namespace={namespace} />
            <div className="flex-1 overflow-auto">
                <Box className="container-inner">
                    <Box className="flex items-center justify-between mb-6">
                        <div>
                            <Typography variant="h5" className="font-medium text-gray-800 dark:text-gray-100 flex items-center">
                                <StorageIcon className="mr-2 text-primary dark:text-primary-light" /> 
                                {cluster}
                                <Chip 
                                    label={`${shardsData.length} shards`} 
                                    size="small" 
                                    color="primary" 
                                    className="ml-3"
                                />
                            </Typography>
                            <Typography variant="body2" className="text-gray-500 dark:text-gray-400 mt-1">
                                Cluster in namespace: {namespace}
                            </Typography>
                        </div>
                    </Box>

                    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
                        <Box className="col-span-1">
                            <AddShardCard namespace={namespace} cluster={cluster} />
                        </Box>
                        
                        {shardsData.length > 0 ? (
                            shardsData.map((shard, index) => (
                                <Link
                                    key={index}
                                    href={`/namespaces/${namespace}/clusters/${cluster}/shards/${index}`}
                                    className="col-span-1"
                                >
                                    <ResourceCard
                                        title={`Shard ${index + 1}`}
                                        tags={[
                                            { label: `${shard.nodes.length} nodes`, color: "secondary" },
                                            shard.migrating_slot >= 0 ? { label: "Migrating", color: "warning" } : undefined
                                        ].filter(Boolean)}
                                    >
                                        <div className="space-y-2 text-sm">
                                            <div className="flex justify-between">
                                                <span className="text-gray-500 dark:text-gray-400">Slots:</span>
                                                <span className="font-medium">{formatSlotRanges(shard.slot_ranges)}</span>
                                            </div>
                                            
                                            {shard.target_shard_index >= 0 && (
                                                <div className="flex justify-between">
                                                    <span className="text-gray-500 dark:text-gray-400">Target Shard:</span>
                                                    <span className="font-medium">{shard.target_shard_index + 1}</span>
                                                </div>
                                            )}
                                            
                                            {shard.migrating_slot >= 0 && (
                                                <div className="flex justify-between">
                                                    <span className="text-gray-500 dark:text-gray-400">Migrating Slot:</span>
                                                    <Badge color="warning" variant="dot">
                                                        <span className="font-medium">{shard.migrating_slot}</span>
                                                    </Badge>
                                                </div>
                                            )}
                                        </div>
                                    </ResourceCard>
                                </Link>
                            ))
                        ) : (
                            <Box className="col-span-full">
                                <EmptyState
                                    title="No shards found"
                                    description="Create a shard to get started"
                                    icon={<DnsIcon sx={{ fontSize: 60 }} />}
                                />
                            </Box>
                        )}
                    </div>
                </Box>
            </div>
        </div>
    );
}
