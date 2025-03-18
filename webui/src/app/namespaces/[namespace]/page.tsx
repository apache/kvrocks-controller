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

import { Box, Typography, Chip } from "@mui/material";
import { NamespaceSidebar } from "../../ui/sidebar";
import { AddClusterCard, ResourceCard } from "../../ui/createCard";
import { fetchCluster, fetchClusters, fetchNamespaces } from "@/app/lib/api";
import Link from "next/link";
import { useRouter, notFound } from "next/navigation";
import { useState, useEffect } from "react";
import { LoadingSpinner } from "@/app/ui/loadingSpinner";
import StorageIcon from '@mui/icons-material/Storage';
import FolderIcon from '@mui/icons-material/Folder';
import EmptyState from "@/app/ui/emptyState";
import GridViewIcon from '@mui/icons-material/GridView';

export default function Namespace({
    params,
}: {
  params: { namespace: string };
}) {
    const [clusterData, setClusterData] = useState<any[]>([]);
    const [loading, setLoading] = useState<boolean>(true);
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
                const data = await Promise.all(
                    clusters.map((cluster) =>
                        fetchCluster(params.namespace, cluster).catch((error) => {
                            console.error(
                                `Failed to fetch data for cluster ${cluster}:`,
                                error
                            );
                            return null;
                        })
                    )
                );
                setClusterData(data.filter(Boolean));
            } catch (error) {
                console.error("Error fetching data:", error);
            } finally {
                setLoading(false);
            }
        };

        fetchData();
    }, [params.namespace, router]);

    if (loading) {
        return <LoadingSpinner />;
    }

    return (
        <div className="flex h-full">
            <NamespaceSidebar />
            <div className="flex-1 overflow-auto">
                <Box className="container-inner">
                    <Box className="flex items-center justify-between mb-6">
                        <div>
                            <Typography variant="h5" className="font-medium text-gray-800 dark:text-gray-100 flex items-center">
                                <FolderIcon className="mr-2 text-primary dark:text-primary-light" /> 
                                {params.namespace}
                                <Chip 
                                    label={`${clusterData.length} clusters`} 
                                    size="small" 
                                    color="primary" 
                                    className="ml-3"
                                />
                            </Typography>
                            <Typography variant="body2" className="text-gray-500 dark:text-gray-400 mt-1">
                                Namespace
                            </Typography>
                        </div>
                    </Box>

                    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
                        <Box className="col-span-1">
                            <AddClusterCard namespace={params.namespace} />
                        </Box>
                        
                        {clusterData.length > 0 ? (
                            clusterData.map((data, index) => (
                                data && (
                                    <Link
                                        href={`/namespaces/${params.namespace}/clusters/${data.name}`}
                                        key={index}
                                        className="col-span-1"
                                    >
                                        <ResourceCard
                                            title={data.name}
                                            description={`Version: ${data.version}`}
                                            tags={[
                                                { label: `${data.shards.length} shards`, color: "secondary" },
                                                ...(data.shards.some((s: any) => s.migrating_slot >= 0)
                                                    ? [{ label: "Migrating", color: "warning" }]
                                                    : [])
                                            ]}
                                        >
                                            <div className="space-y-2 text-sm my-2">
                                                <div className="flex justify-between">
                                                    <span className="text-gray-500 dark:text-gray-400">Slots:</span>
                                                    <span className="font-medium">
                                                        {data.shards[0]?.slot_ranges.length > 0 ? 
                                                            (data.shards[0].slot_ranges.length > 2 ? 
                                                                `${data.shards[0].slot_ranges[0]}, ${data.shards[0].slot_ranges[1]}, ...` : 
                                                                data.shards[0].slot_ranges.join(', ')) : 
                                                            'None'}
                                                    </span>
                                                </div>
                                                
                                                {data.shards[0]?.target_shard_index >= 0 && (
                                                    <div className="flex justify-between">
                                                        <span className="text-gray-500 dark:text-gray-400">Target Shard:</span>
                                                        <span className="font-medium">{data.shards[0].target_shard_index + 1}</span>
                                                    </div>
                                                )}
                                                
                                                {data.shards[0]?.migrating_slot >= 0 && (
                                                    <div className="flex justify-between">
                                                        <span className="text-gray-500 dark:text-gray-400">Migrating:</span>
                                                        <Chip 
                                                            label={`Slot ${data.shards[0].migrating_slot}`} 
                                                            size="small"
                                                            color="warning"
                                                        />
                                                    </div>
                                                )}
                                            </div>
                                            
                                            <div className="mt-3 flex justify-center">
                                                <GridViewIcon sx={{ fontSize: 40 }} className="text-primary/20 dark:text-primary-light/30" />
                                            </div>
                                        </ResourceCard>
                                    </Link>
                                )
                            ))
                        ) : (
                            <Box className="col-span-full">
                                <EmptyState
                                    title="No clusters found"
                                    description="Create a cluster to get started"
                                    icon={<StorageIcon sx={{ fontSize: 60 }} />}
                                />
                            </Box>
                        )}
                    </div>
                </Box>
            </div>
        </div>
    );
}
