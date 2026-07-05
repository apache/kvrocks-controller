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

import { useCallback, useEffect, useState } from "react";
import { useRouter, usePathname } from "next/navigation";
import { Dialog, DialogContent } from "@mui/material";
import SearchIcon from "@mui/icons-material/Search";
import FolderIcon from "@mui/icons-material/Folder";
import StorageIcon from "@mui/icons-material/Storage";
import DnsIcon from "@mui/icons-material/Dns";
import DeviceHubIcon from "@mui/icons-material/DeviceHub";
import { fetchNamespaces, fetchClusters, listShards, listNodes } from "../lib/api";

type SearchType = "namespace" | "cluster" | "shard" | "node";

interface SearchResult {
    type: SearchType;
    title: string;
    subtitle?: string;
    path: string;
    namespace?: string;
    cluster?: string;
    shard?: string;
}

const iconFor = (type: SearchType) => {
    switch (type) {
        case "namespace":
            return <FolderIcon sx={{ fontSize: 14 }} />;
        case "cluster":
            return <StorageIcon sx={{ fontSize: 14 }} />;
        case "shard":
            return <DnsIcon sx={{ fontSize: 14 }} />;
        case "node":
            return <DeviceHubIcon sx={{ fontSize: 14 }} />;
    }
};

export default function SpotlightSearch() {
    const [open, setOpen] = useState(false);
    const [query, setQuery] = useState("");
    const [results, setResults] = useState<SearchResult[]>([]);
    const [selected, setSelected] = useState(0);
    const [allData, setAllData] = useState<SearchResult[]>([]);
    const [loading, setLoading] = useState(false);
    const router = useRouter();
    const pathname = usePathname();

    const loadAll = useCallback(async () => {
        setLoading(true);
        try {
            const data: SearchResult[] = [];
            const namespaces = await fetchNamespaces();
            for (const ns of namespaces) {
                data.push({
                    type: "namespace",
                    title: ns,
                    path: `/namespaces/${ns}`,
                    namespace: ns,
                });
                const clusters = await fetchClusters(ns);
                for (const cluster of clusters) {
                    data.push({
                        type: "cluster",
                        title: cluster,
                        subtitle: ns,
                        path: `/namespaces/${ns}/clusters/${cluster}`,
                        namespace: ns,
                        cluster,
                    });
                    const shards = await listShards(ns, cluster);
                    for (let i = 0; i < shards.length; i++) {
                        data.push({
                            type: "shard",
                            title: `Shard ${i + 1}`,
                            subtitle: `${cluster} · ${ns}`,
                            path: `/namespaces/${ns}/clusters/${cluster}/shards/${i}`,
                            namespace: ns,
                            cluster,
                            shard: String(i),
                        });
                        const nodes = await listNodes(ns, cluster, String(i));
                        (nodes as any[]).forEach((node, nodeIndex) => {
                            data.push({
                                type: "node",
                                title: node.addr || node.id,
                                subtitle: `${node.role} · Shard ${i + 1} · ${cluster}`,
                                path: `/namespaces/${ns}/clusters/${cluster}/shards/${i}/nodes/${nodeIndex}`,
                                namespace: ns,
                                cluster,
                                shard: String(i),
                            });
                        });
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

    const contextSearch = useCallback(
        (q: string) => {
            const parts = pathname.split("/").filter(Boolean);
            const [, ns, , cluster, , shard] = parts;

            if (!q.trim()) {
                if (shard) {
                    return allData
                        .filter(
                            (i) =>
                                i.type === "node" &&
                                i.namespace === ns &&
                                i.cluster === cluster &&
                                i.shard === shard
                        )
                        .slice(0, 10);
                }
                if (cluster) {
                    return allData
                        .filter(
                            (i) => i.type === "shard" && i.namespace === ns && i.cluster === cluster
                        )
                        .slice(0, 10);
                }
                if (ns) {
                    return allData
                        .filter((i) => i.type === "cluster" && i.namespace === ns)
                        .slice(0, 10);
                }
                return allData.filter((i) => i.type === "namespace").slice(0, 10);
            }

            const lower = q.toLowerCase();
            return allData
                .filter((i) =>
                    `${i.title} ${i.subtitle ?? ""} ${i.type}`.toLowerCase().includes(lower)
                )
                .slice(0, 10);
        },
        [allData, pathname]
    );

    useEffect(() => {
        setResults(contextSearch(query));
        setSelected(0);
    }, [query, contextSearch]);

    const handleSelect = useCallback(
        (result: SearchResult) => {
            router.push(result.path);
            setOpen(false);
            setQuery("");
        },
        [router]
    );

    useEffect(() => {
        const onKey = (e: KeyboardEvent) => {
            if ((e.metaKey || e.ctrlKey) && e.key === "k") {
                e.preventDefault();
                setOpen(true);
                if (!allData.length) loadAll();
            }
            if (e.key === "Escape") {
                setOpen(false);
                setQuery("");
            }
            if (open) {
                if (e.key === "ArrowDown") {
                    e.preventDefault();
                    setSelected((prev) => Math.min(prev + 1, results.length - 1));
                } else if (e.key === "ArrowUp") {
                    e.preventDefault();
                    setSelected((prev) => Math.max(prev - 1, 0));
                } else if (e.key === "Enter" && results[selected]) {
                    e.preventDefault();
                    handleSelect(results[selected]);
                }
            }
        };
        window.addEventListener("keydown", onKey);
        return () => window.removeEventListener("keydown", onKey);
    }, [open, results, selected, allData.length, loadAll, handleSelect]);

    return (
        <Dialog
            open={open}
            onClose={() => {
                setOpen(false);
                setQuery("");
            }}
            maxWidth="sm"
            fullWidth
            PaperProps={{
                sx: {
                    position: "fixed",
                    top: "18vh",
                    m: 0,
                    borderRadius: "10px",
                    width: "560px",
                    maxWidth: "calc(100vw - 32px)",
                },
            }}
            slotProps={{
                backdrop: { sx: { backgroundColor: "rgba(15,17,22,0.4)" } },
            }}
        >
            <DialogContent sx={{ p: 0 }}>
                <div className="flex items-center gap-2 border-b border-border-subtle px-3 py-2 dark:border-border-dark-subtle">
                    <SearchIcon sx={{ fontSize: 15 }} className="text-text-muted" />
                    <input
                        autoFocus
                        value={query}
                        onChange={(e) => setQuery(e.target.value)}
                        placeholder="Search namespaces, clusters, shards, nodes…"
                        className="w-full border-0 bg-transparent text-sm text-text-primary outline-none placeholder:text-text-muted dark:text-text-dark-primary dark:placeholder:text-text-dark-muted"
                    />
                </div>

                <div className="max-h-[420px] overflow-y-auto p-1.5">
                    {loading ? (
                        <div className="px-4 py-6 text-center text-xs text-text-muted dark:text-text-dark-muted">
                            Loading…
                        </div>
                    ) : results.length > 0 ? (
                        <ul className="space-y-0.5">
                            {results.map((result, index) => (
                                <li key={`${result.type}-${result.path}`}>
                                    <button
                                        onClick={() => handleSelect(result)}
                                        onMouseEnter={() => setSelected(index)}
                                        className={`flex w-full items-center gap-2.5 rounded-md px-2 py-1.5 text-left transition-colors ${
                                            index === selected
                                                ? "bg-surface-hover dark:bg-surface-dark-hover"
                                                : "hover:bg-surface-hover dark:hover:bg-surface-dark-hover"
                                        }`}
                                    >
                                        <span className="flex h-5 w-5 shrink-0 items-center justify-center rounded text-text-muted dark:text-text-dark-muted">
                                            {iconFor(result.type)}
                                        </span>
                                        <span className="min-w-0 flex-1">
                                            <span className="block truncate text-sm text-text-primary dark:text-text-dark-primary">
                                                {result.title}
                                            </span>
                                            {result.subtitle && (
                                                <span className="block truncate text-xs text-text-muted dark:text-text-dark-muted">
                                                    {result.subtitle}
                                                </span>
                                            )}
                                        </span>
                                        <span className="lin-eyebrow shrink-0 text-2xs">
                                            {result.type}
                                        </span>
                                    </button>
                                </li>
                            ))}
                        </ul>
                    ) : (
                        <div className="px-4 py-8 text-center text-xs text-text-muted dark:text-text-dark-muted">
                            {query ? "No results" : "Start typing to search"}
                        </div>
                    )}
                </div>

                <div className="flex items-center gap-4 border-t border-border-subtle bg-surface-subtle px-3 py-1.5 text-2xs text-text-muted dark:border-border-dark-subtle dark:bg-surface-dark-subtle dark:text-text-dark-muted">
                    <span className="flex items-center gap-1">
                        <kbd>↑</kbd>
                        <kbd>↓</kbd>
                        <span>Navigate</span>
                    </span>
                    <span className="flex items-center gap-1">
                        <kbd>↵</kbd>
                        <span>Open</span>
                    </span>
                    <span className="flex items-center gap-1">
                        <kbd>Esc</kbd>
                        <span>Close</span>
                    </span>
                </div>
            </DialogContent>
        </Dialog>
    );
}
