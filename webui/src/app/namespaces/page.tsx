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

import { Container, Typography, Paper, Box } from "@mui/material";
import { NamespaceSidebar } from "../ui/sidebar";
import { useRouter } from "next/navigation";
import { useState, useEffect } from "react";
import { fetchNamespaces } from "../lib/api";
import { LoadingSpinner } from "../ui/loadingSpinner";
import { CreateCard, ResourceCard } from "../ui/createCard";
import Link from "next/link";
import FolderIcon from '@mui/icons-material/Folder';
import EmptyState from "../ui/emptyState";

export default function Namespace() {
    const [namespaces, setNamespaces] = useState<string[]>([]);
    const [loading, setLoading] = useState<boolean>(true);
    const router = useRouter();

    useEffect(() => {
        const fetchData = async () => {
            try {
                const fetchedNamespaces = await fetchNamespaces();
                setNamespaces(fetchedNamespaces);
            } catch (error) {
                console.error("Error fetching namespaces:", error);
            } finally {
                setLoading(false);
            }
        };

        fetchData();
    }, [router]);

    if (loading) {
        return <LoadingSpinner />;
    }

    return (
        <div className="flex h-full">
            <NamespaceSidebar />
            <div className="flex-1 overflow-auto">
                <Box className="container-inner">
                    <Box className="flex items-center justify-between mb-6">
                        <Typography variant="h5" className="font-medium text-gray-800 dark:text-gray-100">
                            Namespaces
                        </Typography>
                    </Box>

                    {namespaces.length > 0 ? (
                        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
                            {namespaces.map((namespace) => (
                                <Link key={namespace} href={`/namespaces/${namespace}`} passHref>
                                    <ResourceCard 
                                        title={namespace} 
                                        description="Namespace" 
                                        tags={[{ label: "namespace", color: "primary" }]}
                                    >
                                        <div className="flex items-center justify-center h-20 mt-4">
                                            <FolderIcon 
                                                sx={{ fontSize: 60 }} 
                                                className="text-primary/20 dark:text-primary-light/30" 
                                            />
                                        </div>
                                    </ResourceCard>
                                </Link>
                            ))}
                        </div>
                    ) : (
                        <EmptyState
                            title="No namespaces found"
                            description="Create a namespace to get started"
                            icon={<FolderIcon sx={{ fontSize: 60 }} />}
                        />
                    )}
                </Box>
            </div>
        </div>
    );
}
