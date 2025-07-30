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

import { listNodes } from "@/app/lib/api";
import { NodeSidebar } from "@/app/ui/sidebar";
import { Box, Typography, Chip, Paper, Divider, Grid, Alert, IconButton } from "@mui/material";
import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { LoadingSpinner } from "@/app/ui/loadingSpinner";
import { truncateText } from "@/app/utils";
import DeviceHubIcon from "@mui/icons-material/DeviceHub";
import LockIcon from "@mui/icons-material/Lock";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import AccessTimeIcon from "@mui/icons-material/AccessTime";
import CheckCircleIcon from "@mui/icons-material/CheckCircle";
import StorageIcon from "@mui/icons-material/Storage";
import DnsIcon from "@mui/icons-material/Dns";
import InfoIcon from "@mui/icons-material/Info";
import SettingsIcon from "@mui/icons-material/Settings";
import NetworkCheckIcon from "@mui/icons-material/NetworkCheck";
import SecurityIcon from "@mui/icons-material/Security";
import LinkIcon from "@mui/icons-material/Link";

export default function Node({
    params,
}: {
    params: { namespace: string; cluster: string; shard: string; node: string };
}) {
    const { namespace, cluster, shard, node } = params;
    const router = useRouter();
    const [nodeData, setNodeData] = useState<any[]>([]);
    const [loading, setLoading] = useState<boolean>(true);
    const [copied, setCopied] = useState<string | null>(null);

    useEffect(() => {
        const fetchData = async () => {
            try {
                const fetchedNodes = await listNodes(namespace, cluster, shard);
                if (!fetchedNodes) {
                    console.error(`Shard ${shard} not found`);
                    router.push("/404");
                    return;
                }
                setNodeData(fetchedNodes);
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

    const currentNode = nodeData[parseInt(node)];
    if (!currentNode) {
        return (
            <div className="flex h-full">
                <NodeSidebar namespace={namespace} cluster={cluster} shard={shard} />
                <Box className="container-inner flex flex-1 items-center justify-center">
                    <Alert severity="error" variant="filled" className="shadow-lg">
                        Node not found
                    </Alert>
                </Box>
            </div>
        );
    }

    // Get role color and text style
    const getRoleStyles = (role: string) => {
        if (role === "master") {
            return {
                color: "success",
                textClass: "text-success font-medium",
                icon: <CheckCircleIcon fontSize="small" className="mr-1" />,
                bgClass: "bg-green-50 dark:bg-green-900/30",
                borderClass: "border-green-200 dark:border-green-800",
                textColor: "text-green-700 dark:text-green-300",
            };
        }
        return {
            color: "info",
            textClass: "text-info font-medium",
            icon: <DeviceHubIcon fontSize="small" className="mr-1" />,
            bgClass: "bg-blue-50 dark:bg-blue-900/30",
            borderClass: "border-blue-200 dark:border-blue-800",
            textColor: "text-blue-700 dark:text-blue-300",
        };
    };

    const copyToClipboard = (text: string, type: string) => {
        navigator.clipboard.writeText(text);
        setCopied(type);
        setTimeout(() => setCopied(null), 2000);
    };

    const formattedDate = new Date(currentNode.created_at * 1000).toLocaleString();
    const roleStyles = getRoleStyles(currentNode.role);

    return (
        <div className="flex h-full">
            <NodeSidebar namespace={namespace} cluster={cluster} shard={shard} />
            <div className="no-scrollbar flex-1 overflow-y-auto bg-white pb-8 dark:bg-dark">
                <Box className="px-6 py-4 sm:px-8 sm:py-6">
                    {/* Header Section */}
                    <div className="mb-6 flex flex-col gap-4 sm:mb-8 lg:flex-row lg:items-center lg:justify-between">
                        <div>
                            <Typography
                                variant="h4"
                                className="flex items-center font-medium text-gray-900 dark:text-white"
                            >
                                <div className="mr-3 flex h-12 w-12 items-center justify-center rounded-2xl bg-gradient-to-br from-blue-50 to-indigo-50 text-blue-500 dark:from-blue-900/30 dark:to-indigo-900/30 dark:text-blue-400">
                                    <DeviceHubIcon sx={{ fontSize: 28 }} />
                                </div>
                                Node {parseInt(node) + 1}
                                <div
                                    className={`ml-3 flex items-center gap-1 rounded-full border px-3 py-1 ${roleStyles.bgClass} ${roleStyles.borderClass}`}
                                >
                                    {roleStyles.icon}
                                    <span className={`text-sm font-medium ${roleStyles.textColor}`}>
                                        {currentNode.role}
                                    </span>
                                </div>
                            </Typography>
                            <Typography
                                variant="body1"
                                className="mt-2 text-gray-500 dark:text-gray-400"
                            >
                                Shard {parseInt(shard) + 1} • {cluster} cluster • {namespace}{" "}
                                namespace
                            </Typography>
                        </div>
                    </div>

                    {/* Node Details Section */}
                    <Paper
                        elevation={0}
                        className="overflow-hidden rounded-2xl border border-gray-100 transition-all hover:shadow-md dark:border-gray-800 dark:bg-dark-paper"
                    >
                        <div className="border-b border-gray-100 px-6 py-4 dark:border-gray-800 sm:px-8">
                            <Typography
                                variant="h6"
                                className="flex items-center font-medium text-gray-800 dark:text-gray-100"
                            >
                                <SettingsIcon className="mr-2 text-primary dark:text-primary-light" />
                                Node Configuration
                            </Typography>
                        </div>

                        <div className="p-6 sm:p-8">
                            <Grid container spacing={4}>
                                <Grid item xs={12} lg={6}>
                                    <div className="space-y-6">
                                        <div>
                                            <Typography
                                                variant="subtitle2"
                                                className="mb-2 flex items-center text-gray-500 dark:text-gray-400"
                                            >
                                                <LinkIcon fontSize="small" className="mr-1" />
                                                Node ID
                                            </Typography>
                                            <div className="flex items-center">
                                                <div className="flex-1 overflow-hidden rounded-xl bg-gray-50 px-4 py-3 font-mono text-sm dark:bg-gray-800/50">
                                                    <Typography
                                                        variant="body1"
                                                        className="truncate text-gray-800 dark:text-gray-200"
                                                    >
                                                        {currentNode.id}
                                                    </Typography>
                                                </div>
                                                <IconButton
                                                    onClick={() =>
                                                        copyToClipboard(currentNode.id, "id")
                                                    }
                                                    className="ml-3 rounded-full bg-gray-100 p-2 text-gray-500 transition-all hover:bg-gray-200 hover:text-primary dark:bg-gray-700 dark:text-gray-400 dark:hover:bg-gray-600 dark:hover:text-primary-light"
                                                    title="Copy ID"
                                                >
                                                    {copied === "id" ? (
                                                        <CheckCircleIcon
                                                            fontSize="small"
                                                            className="text-success"
                                                        />
                                                    ) : (
                                                        <ContentCopyIcon fontSize="small" />
                                                    )}
                                                </IconButton>
                                            </div>
                                        </div>

                                        <div>
                                            <Typography
                                                variant="subtitle2"
                                                className="mb-2 flex items-center text-gray-500 dark:text-gray-400"
                                            >
                                                <NetworkCheckIcon
                                                    fontSize="small"
                                                    className="mr-1"
                                                />
                                                Address
                                            </Typography>
                                            <div className="flex items-center">
                                                <div className="flex-1 overflow-hidden rounded-xl bg-gray-50 px-4 py-3 dark:bg-gray-800/50">
                                                    <Typography
                                                        variant="body1"
                                                        className="text-gray-800 dark:text-gray-200"
                                                    >
                                                        {currentNode.addr}
                                                    </Typography>
                                                </div>
                                                <IconButton
                                                    onClick={() =>
                                                        copyToClipboard(currentNode.addr, "addr")
                                                    }
                                                    className="ml-3 rounded-full bg-gray-100 p-2 text-gray-500 transition-all hover:bg-gray-200 hover:text-primary dark:bg-gray-700 dark:text-gray-400 dark:hover:bg-gray-600 dark:hover:text-primary-light"
                                                    title="Copy Address"
                                                >
                                                    {copied === "addr" ? (
                                                        <CheckCircleIcon
                                                            fontSize="small"
                                                            className="text-success"
                                                        />
                                                    ) : (
                                                        <ContentCopyIcon fontSize="small" />
                                                    )}
                                                </IconButton>
                                            </div>
                                        </div>
                                    </div>
                                </Grid>

                                <Grid item xs={12} lg={6}>
                                    <div className="space-y-6">
                                        <div>
                                            <Typography
                                                variant="subtitle2"
                                                className="mb-2 flex items-center text-gray-500 dark:text-gray-400"
                                            >
                                                <DeviceHubIcon fontSize="small" className="mr-1" />
                                                Role
                                            </Typography>
                                            <div
                                                className={`mt-1 inline-flex items-center rounded-xl border px-3 py-2 ${roleStyles.bgClass} ${roleStyles.borderClass}`}
                                            >
                                                {roleStyles.icon}
                                                <Typography
                                                    variant="body1"
                                                    className={`font-medium ${roleStyles.textColor}`}
                                                >
                                                    {currentNode.role}
                                                </Typography>
                                            </div>
                                        </div>

                                        <div>
                                            <Typography
                                                variant="subtitle2"
                                                className="mb-2 flex items-center text-gray-500 dark:text-gray-400"
                                            >
                                                <AccessTimeIcon fontSize="small" className="mr-1" />
                                                Created At
                                            </Typography>
                                            <div className="flex items-center rounded-xl bg-gray-50 px-4 py-3 dark:bg-gray-800/50">
                                                <Typography
                                                    variant="body1"
                                                    className="text-gray-800 dark:text-gray-200"
                                                >
                                                    {formattedDate}
                                                </Typography>
                                            </div>
                                        </div>

                                        {currentNode.password && (
                                            <div>
                                                <Typography
                                                    variant="subtitle2"
                                                    className="mb-2 flex items-center text-gray-500 dark:text-gray-400"
                                                >
                                                    <SecurityIcon
                                                        fontSize="small"
                                                        className="mr-1"
                                                    />
                                                    Authentication
                                                </Typography>
                                                <div className="flex items-center">
                                                    <div className="flex-1 rounded-xl bg-gray-50 px-4 py-3 dark:bg-gray-800/50">
                                                        <Typography
                                                            variant="body2"
                                                            className="font-mono text-gray-800 dark:text-gray-200"
                                                        >
                                                            {currentNode.password
                                                                ? "••••••••"
                                                                : "No password set"}
                                                        </Typography>
                                                    </div>
                                                    <IconButton
                                                        onClick={() =>
                                                            copyToClipboard(
                                                                currentNode.password,
                                                                "pwd"
                                                            )
                                                        }
                                                        className="ml-3 rounded-full bg-gray-100 p-2 text-gray-500 transition-all hover:bg-gray-200 hover:text-primary dark:bg-gray-700 dark:text-gray-400 dark:hover:bg-gray-600 dark:hover:text-primary-light"
                                                        title="Copy Password"
                                                        disabled={!currentNode.password}
                                                    >
                                                        {copied === "pwd" ? (
                                                            <CheckCircleIcon
                                                                fontSize="small"
                                                                className="text-success"
                                                            />
                                                        ) : (
                                                            <LockIcon fontSize="small" />
                                                        )}
                                                    </IconButton>
                                                </div>
                                            </div>
                                        )}
                                    </div>
                                </Grid>
                            </Grid>
                        </div>
                    </Paper>

                    {/* Shard Information Section */}
                    <Paper
                        elevation={0}
                        className="mt-6 overflow-hidden rounded-2xl border border-gray-100 transition-all hover:shadow-md dark:border-gray-800 dark:bg-dark-paper"
                    >
                        <div className="border-b border-gray-100 px-6 py-4 dark:border-gray-800 sm:px-8">
                            <Typography
                                variant="h6"
                                className="flex items-center font-medium text-gray-800 dark:text-gray-100"
                            >
                                <DnsIcon className="mr-2 text-primary dark:text-primary-light" />
                                Shard Information
                            </Typography>
                        </div>

                        <div className="p-6 sm:p-8">
                            <Grid container spacing={4}>
                                <Grid item xs={12} sm={4}>
                                    <div className="rounded-xl border border-gray-100 bg-gray-50 p-4 dark:border-gray-800 dark:bg-gray-800/50">
                                        <Typography
                                            variant="subtitle2"
                                            className="mb-2 flex items-center text-gray-500 dark:text-gray-400"
                                        >
                                            <DnsIcon fontSize="small" className="mr-1" />
                                            Shard
                                        </Typography>
                                        <Typography
                                            variant="h6"
                                            className="font-semibold text-gray-900 dark:text-white"
                                        >
                                            Shard {parseInt(shard) + 1}
                                        </Typography>
                                    </div>
                                </Grid>
                                <Grid item xs={12} sm={4}>
                                    <div className="rounded-xl border border-gray-100 bg-gray-50 p-4 dark:border-gray-800 dark:bg-gray-800/50">
                                        <Typography
                                            variant="subtitle2"
                                            className="mb-2 flex items-center text-gray-500 dark:text-gray-400"
                                        >
                                            <StorageIcon fontSize="small" className="mr-1" />
                                            Cluster
                                        </Typography>
                                        <Typography
                                            variant="h6"
                                            className="font-semibold text-gray-900 dark:text-white"
                                        >
                                            {cluster}
                                        </Typography>
                                    </div>
                                </Grid>
                                <Grid item xs={12} sm={4}>
                                    <div className="rounded-xl border border-gray-100 bg-gray-50 p-4 dark:border-gray-800 dark:bg-gray-800/50">
                                        <Typography
                                            variant="subtitle2"
                                            className="mb-2 flex items-center text-gray-500 dark:text-gray-400"
                                        >
                                            <InfoIcon fontSize="small" className="mr-1" />
                                            Namespace
                                        </Typography>
                                        <Typography
                                            variant="h6"
                                            className="font-semibold text-gray-900 dark:text-white"
                                        >
                                            {namespace}
                                        </Typography>
                                    </div>
                                </Grid>
                            </Grid>
                        </div>
                    </Paper>
                </Box>
            </div>
        </div>
    );
}
