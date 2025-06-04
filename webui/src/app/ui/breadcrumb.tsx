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

import { Box, Typography, Chip, Paper } from "@mui/material";
import { usePathname } from "next/navigation";
import Link from "next/link";
import { useState, useEffect } from "react";
import HomeIcon from "@mui/icons-material/Home";
import ChevronRightIcon from "@mui/icons-material/ChevronRight";
import FolderIcon from "@mui/icons-material/Folder";
import StorageIcon from "@mui/icons-material/Storage";
import DnsIcon from "@mui/icons-material/Dns";
import DeviceHubIcon from "@mui/icons-material/DeviceHub";
import { fetchClusters, listShards, listNodes } from "@/app/lib/api";

interface BreadcrumbItem {
    name: string;
    displayName: string;
    url: string;
    icon: React.ReactNode | null;
    isNumeric: boolean;
    isLast: boolean;
}

export default function Breadcrumb() {
    const pathname = usePathname();
    const [breadcrumbItems, setBreadcrumbItems] = useState<BreadcrumbItem[]>([]);
    const [loading, setLoading] = useState(false);

    useEffect(() => {
        if (pathname === "/") {
            setBreadcrumbItems([]);
            setLoading(false);
            return;
        }

        const generateBreadcrumbs = async () => {
            setLoading(true);
            const pathSegments = pathname.split("/").filter(Boolean);

            if (pathSegments.length === 0) {
                setBreadcrumbItems([]);
                setLoading(false);
                return;
            }

            const items: BreadcrumbItem[] = [];

            for (let index = 0; index < pathSegments.length; index++) {
                const segment = pathSegments[index];
                const url = `/${pathSegments.slice(0, index + 1).join("/")}`;
                const isLast = index === pathSegments.length - 1;
                const isNumeric = !isNaN(Number(segment));

                let icon = null;
                let displayName = segment;

                if (index === 0 && segment === "namespaces") {
                    icon = <FolderIcon fontSize="small" className="text-primary/70" />;
                    displayName = "Namespaces";
                } else if (segment === "clusters") {
                    icon = <StorageIcon fontSize="small" className="text-primary/70" />;
                    displayName = "Clusters";
                } else if (segment === "shards") {
                    icon = <DnsIcon fontSize="small" className="text-primary/70" />;
                    displayName = "Shards";
                } else if (segment === "nodes") {
                    icon = <DeviceHubIcon fontSize="small" className="text-primary/70" />;
                    displayName = "Nodes";
                } else if (isNumeric) {
                    const prevSegment = pathSegments[index - 1];
                    if (prevSegment === "shards") {
                        displayName = `Shard ${parseInt(segment) + 1}`;
                        icon = null;
                    } else if (prevSegment === "nodes") {
                        displayName = `Node ${parseInt(segment) + 1}`;
                        icon = null;
                    } else {
                        displayName = `ID: ${segment}`;
                        icon = null;
                    }
                } else {
                    // For namespace and cluster names, capitalize first letter
                    displayName = segment.charAt(0).toUpperCase() + segment.slice(1);

                    const prevSegment = pathSegments[index - 1];
                    if (prevSegment === "namespaces") {
                        icon = null;
                    } else if (prevSegment === "clusters") {
                        icon = null;
                    }
                }

                items.push({
                    name: segment,
                    displayName,
                    url,
                    icon,
                    isNumeric,
                    isLast,
                });
            }

            setBreadcrumbItems(items);
            setLoading(false);
        };

        generateBreadcrumbs();
    }, [pathname]);

    if (pathname === "/" || breadcrumbItems.length === 0) return null;

    return (
        <Paper elevation={0} className="mt-4 w-full border-0 bg-white px-6 py-3 dark:bg-dark-paper">
            <Box className="flex items-center overflow-x-auto py-1 pt-2">
                <Link
                    href="/"
                    className="flex items-center text-gray-600 transition-colors hover:text-primary dark:text-gray-300 dark:hover:text-primary-light"
                >
                    <HomeIcon fontSize="small" className="mr-2" />
                    <Typography variant="body2" className="font-medium">
                        Home
                    </Typography>
                </Link>

                {breadcrumbItems.map((item, index) => (
                    <Box key={index} className="flex items-center">
                        <ChevronRightIcon
                            fontSize="small"
                            className="mx-3 text-gray-400 dark:text-gray-500"
                        />

                        {item.isLast ? (
                            <Box className="flex items-center">
                                {item.icon && <span className="mr-2">{item.icon}</span>}
                                {item.isNumeric ? (
                                    <Chip
                                        label={item.displayName}
                                        size="small"
                                        className="border border-primary/20 font-medium dark:border-primary-dark/30"
                                        sx={{
                                            height: "24px",
                                            backgroundColor: (theme) =>
                                                theme.palette.mode === "dark"
                                                    ? "rgba(21, 101, 192, 0.95)"
                                                    : "rgba(25, 118, 210, 0.08)",
                                            color: (theme) =>
                                                theme.palette.mode === "dark"
                                                    ? "#ffffff"
                                                    : "#1976d2",
                                            "& .MuiChip-label": {
                                                px: 1.5,
                                                fontSize: "0.75rem",
                                                fontWeight: 600,
                                            },
                                        }}
                                    />
                                ) : (
                                    <Typography
                                        variant="body2"
                                        className="font-semibold"
                                        sx={{
                                            backgroundColor: (theme) =>
                                                theme.palette.mode === "dark"
                                                    ? "rgba(21, 101, 192, 0.95)"
                                                    : "rgba(25, 118, 210, 0.08)",
                                            color: (theme) =>
                                                theme.palette.mode === "dark"
                                                    ? "#ffffff"
                                                    : "#1976d2",
                                            padding: "4px 10px",
                                            borderRadius: "16px",
                                        }}
                                    >
                                        {item.displayName}
                                    </Typography>
                                )}
                            </Box>
                        ) : item.name === "clusters" ||
                          item.name === "shards" ||
                          item.name === "nodes" ? (
                            <Box className="flex items-center">
                                {item.icon && <span className="mr-2">{item.icon}</span>}
                                <Typography
                                    variant="body2"
                                    className="font-medium text-gray-500 dark:text-gray-400"
                                >
                                    {item.displayName}
                                </Typography>
                            </Box>
                        ) : (
                            <Link
                                href={item.url}
                                className="flex items-center text-gray-600 transition-colors hover:text-primary dark:text-gray-300 dark:hover:text-primary-light"
                            >
                                {item.icon && <span className="mr-2">{item.icon}</span>}
                                <Typography variant="body2" className="font-medium">
                                    {item.displayName}
                                </Typography>
                            </Link>
                        )}
                    </Box>
                ))}
            </Box>
        </Paper>
    );
}
