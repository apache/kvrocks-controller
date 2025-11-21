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

import { useEffect, useState, useCallback } from "react";
import { useRouter, usePathname } from "next/navigation";
import {
    Dialog,
    DialogContent,
    TextField,
    List,
    ListItem,
    ListItemButton,
    Box,
    Typography,
    Chip,
    alpha,
} from "@mui/material";
import SearchIcon from "@mui/icons-material/Search";
import FolderIcon from "@mui/icons-material/Folder";
import StorageIcon from "@mui/icons-material/Storage";
import DnsIcon from "@mui/icons-material/Dns";
import DeviceHubIcon from "@mui/icons-material/DeviceHub";
import { fetchNamespaces, fetchClusters, listShards, listNodes } from "../lib/api";

interface SearchResult {
    type: "namespace" | "cluster" | "shard" | "node";
    title: string;
    subtitle?: string;
    path: string;
    namespace?: string;
    cluster?: string;
    shard?: string;
}

export default function SpotlightSearch() {
    const [open, setOpen] = useState(false);
    const [query, setQuery] = useState("");
    const [results, setResults] = useState<SearchResult[]>([]);
    const [selectedIndex, setSelectedIndex] = useState(0);
    const [allData, setAllData] = useState<SearchResult[]>([]);
    const [loading, setLoading] = useState(false);
    const router = useRouter();
    const pathname = usePathname();

    // Load all data when dialog opens
    const loadAllData = useCallback(async () => {
        setLoading(true);
        try {
            const data: SearchResult[] = [];
            const namespaces = await fetchNamespaces();

            for (const ns of namespaces) {
                // Add namespace
                data.push({
                    type: "namespace",
                    title: ns,
                    path: `/namespaces/${ns}`,
                    namespace: ns,
                });

                // Add clusters
                const clusters = await fetchClusters(ns);
                for (const cluster of clusters) {
                    data.push({
                        type: "cluster",
                        title: cluster,
                        subtitle: `in ${ns}`,
                        path: `/namespaces/${ns}/clusters/${cluster}`,
                        namespace: ns,
                        cluster,
                    });

                    // Add shards
                    const shards = await listShards(ns, cluster);
                    for (let i = 0; i < shards.length; i++) {
                        data.push({
                            type: "shard",
                            title: `Shard ${i}`,
                            subtitle: `${cluster} / ${ns}`,
                            path: `/namespaces/${ns}/clusters/${cluster}/shards/${i}`,
                            namespace: ns,
                            cluster,
                            shard: String(i),
                        });

                        // Add nodes
                        const nodes = await listNodes(ns, cluster, String(i));
                        for (let nodeIndex = 0; nodeIndex < (nodes as any[]).length; nodeIndex++) {
                            const node = (nodes as any[])[nodeIndex];
                            data.push({
                                type: "node",
                                title: node.addr || node.id,
                                subtitle: `${node.role} in Shard ${i} / ${cluster}`,
                                path: `/namespaces/${ns}/clusters/${cluster}/shards/${i}/nodes/${nodeIndex}`,
                                namespace: ns,
                                cluster,
                                shard: String(i),
                            });
                        }
                    }
                }
            }

            setAllData(data);
        } catch (error) {
            console.error("Failed to load search data:", error);
        } finally {
            setLoading(false);
        }
    }, []);

    // Context-aware search function
    const contextAwareSearch = useCallback(
        (searchQuery: string) => {
            // Parse current context from pathname
            const pathParts = pathname.split("/").filter(Boolean);
            const currentNamespace = pathParts[1];
            const currentCluster = pathParts[3];
            const currentShard = pathParts[5];

            // If no query, show context-relevant items only
            if (!searchQuery.trim()) {
                let contextData: SearchResult[] = [];

                if (currentShard) {
                    // In shard page - show only nodes in this shard
                    contextData = allData.filter(
                        (item) =>
                            item.type === "node" &&
                            item.namespace === currentNamespace &&
                            item.cluster === currentCluster &&
                            item.shard === currentShard
                    );
                } else if (currentCluster) {
                    // In cluster page - show only shards in this cluster
                    contextData = allData.filter(
                        (item) =>
                            item.type === "shard" &&
                            item.namespace === currentNamespace &&
                            item.cluster === currentCluster
                    );
                } else if (currentNamespace) {
                    // In namespace page - show only clusters in this namespace
                    contextData = allData.filter(
                        (item) => item.type === "cluster" && item.namespace === currentNamespace
                    );
                } else {
                    // On home/namespaces page - show only namespaces
                    contextData = allData.filter((item) => item.type === "namespace");
                }

                return contextData.slice(0, 10);
            }

            // With query, search everything
            const lowerQuery = searchQuery.toLowerCase();
            const filtered = allData.filter((item) => {
                const searchText =
                    `${item.title} ${item.subtitle || ""} ${item.type}`.toLowerCase();
                return searchText.includes(lowerQuery);
            });

            return filtered.slice(0, 10);
        },
        [allData, pathname]
    );

    // Update results when query or pathname changes
    useEffect(() => {
        const filtered = contextAwareSearch(query);
        setResults(filtered);
        setSelectedIndex(0);
    }, [query, contextAwareSearch]);

    // Handle keyboard shortcuts
    useEffect(() => {
        const handleKeyDown = (e: KeyboardEvent) => {
            // Cmd+K or Ctrl+K to open
            if ((e.metaKey || e.ctrlKey) && e.key === "k") {
                e.preventDefault();
                setOpen(true);
                if (!allData.length) {
                    loadAllData();
                }
            }

            // Escape to close
            if (e.key === "Escape") {
                setOpen(false);
                setQuery("");
            }

            // Arrow navigation when open
            if (open) {
                if (e.key === "ArrowDown") {
                    e.preventDefault();
                    setSelectedIndex((prev) => Math.min(prev + 1, results.length - 1));
                } else if (e.key === "ArrowUp") {
                    e.preventDefault();
                    setSelectedIndex((prev) => Math.max(prev - 1, 0));
                } else if (e.key === "Enter" && results[selectedIndex]) {
                    e.preventDefault();
                    handleSelect(results[selectedIndex]);
                }
            }
        };

        window.addEventListener("keydown", handleKeyDown);
        return () => window.removeEventListener("keydown", handleKeyDown);
    }, [open, results, selectedIndex, allData.length, loadAllData]);

    const handleSelect = (result: SearchResult) => {
        router.push(result.path);
        setOpen(false);
        setQuery("");
    };

    const handleClose = () => {
        setOpen(false);
        setQuery("");
    };

    const getTypeIcon = (type: string) => {
        switch (type) {
            case "namespace":
                return <FolderIcon sx={{ fontSize: 18 }} />;
            case "cluster":
                return <StorageIcon sx={{ fontSize: 18 }} />;
            case "shard":
                return <DnsIcon sx={{ fontSize: 18 }} />;
            case "node":
                return <DeviceHubIcon sx={{ fontSize: 18 }} />;
            default:
                return null;
        }
    };

    const getTypeColor = (type: string) => {
        switch (type) {
            case "namespace":
                return "#3b82f6"; // blue
            case "cluster":
                return "#8b5cf6"; // purple
            case "shard":
                return "#10b981"; // green
            case "node":
                return "#f59e0b"; // orange
            default:
                return "#6b7280";
        }
    };

    return (
        <Dialog
            open={open}
            onClose={handleClose}
            maxWidth="md"
            fullWidth
            PaperProps={{
                sx: {
                    position: "fixed",
                    top: "15%",
                    m: 0,
                    borderRadius: 4,
                    boxShadow: "0 25px 50px -12px rgba(0, 0, 0, 0.25)",
                    overflow: "hidden",
                    backdropFilter: "blur(20px)",
                    background: (theme) =>
                        theme.palette.mode === "dark"
                            ? "rgba(30, 30, 30, 0.95)"
                            : "rgba(255, 255, 255, 0.95)",
                },
            }}
            slotProps={{
                backdrop: {
                    sx: {
                        backdropFilter: "blur(4px)",
                        backgroundColor: "rgba(0, 0, 0, 0.3)",
                    },
                },
            }}
        >
            <DialogContent sx={{ p: 0 }}>
                <Box
                    sx={{
                        p: 3,
                        borderBottom: 1,
                        borderColor: (theme) =>
                            theme.palette.mode === "dark"
                                ? "rgba(255, 255, 255, 0.08)"
                                : "rgba(0, 0, 0, 0.08)",
                    }}
                >
                    <Box sx={{ display: "flex", alignItems: "center", gap: 2 }}>
                        <SearchIcon
                            sx={{
                                fontSize: 24,
                                color: "text.secondary",
                                opacity: 0.6,
                            }}
                        />
                        <TextField
                            fullWidth
                            autoFocus
                            placeholder={
                                query
                                    ? "Search everything..."
                                    : "Search in current context (or type to search all)..."
                            }
                            value={query}
                            onChange={(e) => setQuery(e.target.value)}
                            variant="standard"
                            InputProps={{
                                disableUnderline: true,
                                sx: {
                                    fontSize: "1.125rem",
                                    fontWeight: 400,
                                    "& input::placeholder": {
                                        opacity: 0.6,
                                    },
                                },
                            }}
                        />
                    </Box>
                </Box>

                {loading ? (
                    <Box sx={{ p: 8, textAlign: "center" }}>
                        <Typography color="text.secondary" sx={{ opacity: 0.7 }}>
                            Loading resources...
                        </Typography>
                    </Box>
                ) : results.length > 0 ? (
                    <List sx={{ maxHeight: 420, overflow: "auto", p: 2 }}>
                        {results.map((result, index) => (
                            <ListItem
                                key={`${result.type}-${result.path}`}
                                disablePadding
                                sx={{ mb: 1 }}
                            >
                                <ListItemButton
                                    selected={index === selectedIndex}
                                    onClick={() => handleSelect(result)}
                                    sx={{
                                        borderRadius: 3,
                                        px: 2.5,
                                        py: 1.5,
                                        transition: "all 0.2s cubic-bezier(0.4, 0, 0.2, 1)",
                                        "&:hover": {
                                            transform: "translateX(4px)",
                                            bgcolor: (theme) =>
                                                theme.palette.mode === "dark"
                                                    ? alpha(getTypeColor(result.type), 0.15)
                                                    : alpha(getTypeColor(result.type), 0.08),
                                        },
                                        "&.Mui-selected": {
                                            bgcolor: (theme) =>
                                                theme.palette.mode === "dark"
                                                    ? alpha(getTypeColor(result.type), 0.2)
                                                    : alpha(getTypeColor(result.type), 0.12),
                                            "&:hover": {
                                                bgcolor: (theme) =>
                                                    theme.palette.mode === "dark"
                                                        ? alpha(getTypeColor(result.type), 0.25)
                                                        : alpha(getTypeColor(result.type), 0.15),
                                            },
                                        },
                                    }}
                                >
                                    <Box
                                        sx={{
                                            display: "flex",
                                            alignItems: "center",
                                            gap: 2,
                                            width: "100%",
                                        }}
                                    >
                                        <Box
                                            sx={{
                                                display: "flex",
                                                alignItems: "center",
                                                justifyContent: "center",
                                                width: 40,
                                                height: 40,
                                                borderRadius: 2.5,
                                                bgcolor: (theme) =>
                                                    theme.palette.mode === "dark"
                                                        ? alpha(getTypeColor(result.type), 0.2)
                                                        : alpha(getTypeColor(result.type), 0.12),
                                                color: getTypeColor(result.type),
                                                flexShrink: 0,
                                            }}
                                        >
                                            {getTypeIcon(result.type)}
                                        </Box>
                                        <Box sx={{ flex: 1, minWidth: 0 }}>
                                            <Typography
                                                variant="body1"
                                                sx={{
                                                    fontWeight: 500,
                                                    fontSize: "0.95rem",
                                                    mb: result.subtitle ? 0.25 : 0,
                                                    overflow: "hidden",
                                                    textOverflow: "ellipsis",
                                                    whiteSpace: "nowrap",
                                                }}
                                            >
                                                {result.title}
                                            </Typography>
                                            {result.subtitle && (
                                                <Typography
                                                    variant="caption"
                                                    sx={{
                                                        color: "text.secondary",
                                                        fontSize: "0.8rem",
                                                        opacity: 0.7,
                                                        overflow: "hidden",
                                                        textOverflow: "ellipsis",
                                                        whiteSpace: "nowrap",
                                                        display: "block",
                                                    }}
                                                >
                                                    {result.subtitle}
                                                </Typography>
                                            )}
                                        </Box>
                                        <Chip
                                            label={result.type}
                                            size="small"
                                            sx={{
                                                height: 24,
                                                fontSize: "0.7rem",
                                                fontWeight: 600,
                                                borderRadius: 2,
                                                bgcolor: (theme) =>
                                                    theme.palette.mode === "dark"
                                                        ? alpha(getTypeColor(result.type), 0.25)
                                                        : alpha(getTypeColor(result.type), 0.15),
                                                color: getTypeColor(result.type),
                                                border: "none",
                                                textTransform: "capitalize",
                                            }}
                                        />
                                    </Box>
                                </ListItemButton>
                            </ListItem>
                        ))}
                    </List>
                ) : (
                    <Box sx={{ p: 8, textAlign: "center" }}>
                        <SearchIcon
                            sx={{
                                fontSize: 48,
                                color: "text.secondary",
                                opacity: 0.3,
                                mb: 2,
                            }}
                        />
                        <Typography
                            color="text.secondary"
                            sx={{ opacity: 0.7, fontSize: "0.95rem" }}
                        >
                            {query ? "No results found" : "Start typing to search all resources..."}
                        </Typography>
                    </Box>
                )}

                <Box
                    sx={{
                        px: 3,
                        py: 2,
                        borderTop: 1,
                        borderColor: (theme) =>
                            theme.palette.mode === "dark"
                                ? "rgba(255, 255, 255, 0.08)"
                                : "rgba(0, 0, 0, 0.08)",
                        display: "flex",
                        gap: 3,
                        justifyContent: "flex-end",
                        bgcolor: (theme) =>
                            theme.palette.mode === "dark"
                                ? "rgba(0, 0, 0, 0.2)"
                                : "rgba(0, 0, 0, 0.02)",
                    }}
                >
                    <Box sx={{ display: "flex", alignItems: "center", gap: 0.75 }}>
                        <Typography
                            variant="caption"
                            sx={{ color: "text.secondary", fontSize: "0.75rem", opacity: 0.7 }}
                        >
                            <kbd>↑↓</kbd> Navigate
                        </Typography>
                    </Box>
                    <Box sx={{ display: "flex", alignItems: "center", gap: 0.75 }}>
                        <Typography
                            variant="caption"
                            sx={{ color: "text.secondary", fontSize: "0.75rem", opacity: 0.7 }}
                        >
                            <kbd>↵</kbd> Select
                        </Typography>
                    </Box>
                    <Box sx={{ display: "flex", alignItems: "center", gap: 0.75 }}>
                        <Typography
                            variant="caption"
                            sx={{ color: "text.secondary", fontSize: "0.75rem", opacity: 0.7 }}
                        >
                            <kbd>Esc</kbd> Close
                        </Typography>
                    </Box>
                </Box>
            </DialogContent>
        </Dialog>
    );
}
