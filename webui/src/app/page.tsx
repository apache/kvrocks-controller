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

import { useRouter } from "next/navigation";
import { Button } from "@mui/material";
import ArrowForwardIcon from "@mui/icons-material/ArrowForward";
import GitHubIcon from "@mui/icons-material/GitHub";
import LaunchIcon from "@mui/icons-material/Launch";
import StorageIcon from "@mui/icons-material/Storage";
import SyncIcon from "@mui/icons-material/Sync";
import SecurityIcon from "@mui/icons-material/Security";
import HubIcon from "@mui/icons-material/Hub";
import SpeedIcon from "@mui/icons-material/Speed";
import CloudIcon from "@mui/icons-material/Cloud";
import MenuBookIcon from "@mui/icons-material/MenuBook";
import Link from "next/link";

const features = [
    {
        title: "Namespaces",
        description: "Isolate tenants with per-namespace auth tokens.",
        icon: <StorageIcon sx={{ fontSize: 18 }} />,
    },
    {
        title: "Replication",
        description: "Binlog-based async replication, like MySQL.",
        icon: <SyncIcon sx={{ fontSize: 18 }} />,
    },
    {
        title: "High availability",
        description: "Automatic failover for master and replica nodes.",
        icon: <SecurityIcon sx={{ fontSize: 18 }} />,
    },
    {
        title: "Cluster",
        description: "Centralized control, standard Redis cluster clients.",
        icon: <HubIcon sx={{ fontSize: 18 }} />,
    },
];

const benefits = [
    {
        title: "Enterprise-grade reliability",
        description: "Fault-tolerant behaviour for mission-critical workloads.",
        icon: <SecurityIcon sx={{ fontSize: 16 }} />,
    },
    {
        title: "High-performance storage",
        description: "RocksDB engine tuned for low-latency access.",
        icon: <SpeedIcon sx={{ fontSize: 16 }} />,
    },
    {
        title: "Simplified operations",
        description: "One interface for the full topology, top to bottom.",
        icon: <HubIcon sx={{ fontSize: 16 }} />,
    },
    {
        title: "Redis-compatible",
        description: "Works with the redis-cli and existing Redis clients.",
        icon: <CloudIcon sx={{ fontSize: 16 }} />,
    },
];

const resources = [
    {
        title: "Documentation",
        description: "Reference for configuration, deployment, and commands.",
        href: "https://kvrocks.apache.org/docs/",
        icon: <MenuBookIcon sx={{ fontSize: 16 }} />,
    },
    {
        title: "GitHub repository",
        description: "Source, issues, and releases.",
        href: "https://github.com/apache/kvrocks-controller",
        icon: <GitHubIcon sx={{ fontSize: 16 }} />,
    },
];

const terminalLines = [
    { text: "$ redis-cli -p 6666", tone: "text-emerald-400" },
    { text: '127.0.0.1:6666> SET mykey "Hello Kvrocks"', tone: "text-primary-light" },
    { text: "OK", tone: "text-amber-300" },
    { text: "127.0.0.1:6666> GET mykey", tone: "text-primary-light" },
    { text: '"Hello Kvrocks"', tone: "text-amber-300" },
    { text: "127.0.0.1:6666> INFO", tone: "text-primary-light" },
    { text: "# Server", tone: "text-amber-200" },
    { text: "kvrocks_version:unstable", tone: "text-amber-300" },
];

export default function Home() {
    const router = useRouter();

    return (
        <div className="lin-fade-in mx-auto flex w-full max-w-[1200px] flex-col gap-16 px-6 py-16">
            <section className="grid grid-cols-1 items-center gap-10 md:grid-cols-2">
                <div className="space-y-5">
                    <span className="lin-eyebrow inline-flex items-center gap-2">
                        <span className="h-1 w-1 rounded-full bg-primary" />
                        Apache Software Foundation
                    </span>
                    <h1 className="text-4xl font-semibold tracking-tight text-text-primary dark:text-text-dark-primary md:text-5xl">
                        Apache Kvrocks
                        <span className="block text-text-muted dark:text-text-dark-muted">
                            Controller
                        </span>
                    </h1>
                    <p className="max-w-md text-sm leading-relaxed text-text-secondary dark:text-text-dark-secondary">
                        A distributed key-value NoSQL database built on RocksDB, compatible with the
                        Redis protocol. This controller manages your Kvrocks clusters at scale.
                    </p>
                    <div className="flex flex-wrap items-center gap-2 pt-2">
                        <Button
                            variant="contained"
                            size="large"
                            onClick={() => router.push("/namespaces")}
                            endIcon={<ArrowForwardIcon sx={{ fontSize: 14 }} />}
                        >
                            Open dashboard
                        </Button>
                        <Button
                            variant="outlined"
                            size="large"
                            href="https://github.com/apache/kvrocks"
                            target="_blank"
                            rel="noopener noreferrer"
                            startIcon={<GitHubIcon sx={{ fontSize: 14 }} />}
                        >
                            GitHub
                        </Button>
                    </div>
                </div>

                <div className="overflow-hidden rounded-lg border border-border-subtle bg-[#0d0e10] dark:border-border-dark-subtle">
                    <div className="flex items-center gap-2 border-b border-white/10 px-3 py-2">
                        <span className="h-2.5 w-2.5 rounded-full bg-[#eb5757]" />
                        <span className="h-2.5 w-2.5 rounded-full bg-[#f2c94c]" />
                        <span className="h-2.5 w-2.5 rounded-full bg-[#4cb782]" />
                        <span className="ml-2 text-2xs uppercase tracking-wider text-white/40">
                            redis-cli
                        </span>
                    </div>
                    <pre className="min-h-[220px] px-4 py-3 font-mono text-xs leading-relaxed text-white/90">
                        {terminalLines.map((line, i) => (
                            <div key={i} className={line.tone}>
                                {line.text}
                            </div>
                        ))}
                    </pre>
                </div>
            </section>

            <section>
                <div className="mb-6">
                    <div className="lin-eyebrow mb-1">Features</div>
                    <h2 className="text-2xl font-semibold text-text-primary dark:text-text-dark-primary">
                        Built for production topologies
                    </h2>
                </div>
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
                    {features.map((f) => (
                        <div
                            key={f.title}
                            className="rounded-lg border border-border-subtle bg-surface-subtle p-4 transition-colors hover:border-border-strong dark:border-border-dark-subtle dark:bg-surface-dark-subtle dark:hover:border-border-dark-strong"
                        >
                            <div className="mb-3 flex h-8 w-8 items-center justify-center rounded-md bg-primary/10 text-primary dark:bg-primary/15 dark:text-primary-light">
                                {f.icon}
                            </div>
                            <div className="text-sm font-semibold text-text-primary dark:text-text-dark-primary">
                                {f.title}
                            </div>
                            <p className="mt-1 text-xs text-text-muted dark:text-text-dark-muted">
                                {f.description}
                            </p>
                        </div>
                    ))}
                </div>
            </section>

            <section>
                <div className="mb-6">
                    <div className="lin-eyebrow mb-1">Why Kvrocks</div>
                    <h2 className="text-2xl font-semibold text-text-primary dark:text-text-dark-primary">
                        Simplified management, without compromise
                    </h2>
                </div>
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                    {benefits.map((b) => (
                        <div
                            key={b.title}
                            className="flex items-start gap-3 rounded-lg border border-border-subtle bg-surface-subtle p-4 dark:border-border-dark-subtle dark:bg-surface-dark-subtle"
                        >
                            <div className="flex h-7 w-7 shrink-0 items-center justify-center rounded-md bg-surface-muted text-text-secondary dark:bg-surface-dark-muted dark:text-text-dark-secondary">
                                {b.icon}
                            </div>
                            <div>
                                <div className="text-sm font-medium text-text-primary dark:text-text-dark-primary">
                                    {b.title}
                                </div>
                                <p className="mt-0.5 text-xs text-text-muted dark:text-text-dark-muted">
                                    {b.description}
                                </p>
                            </div>
                        </div>
                    ))}
                </div>
            </section>

            <section>
                <div className="mb-6">
                    <div className="lin-eyebrow mb-1">Resources</div>
                    <h2 className="text-2xl font-semibold text-text-primary dark:text-text-dark-primary">
                        Docs and source
                    </h2>
                </div>
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                    {resources.map((r) => (
                        <Link
                            key={r.title}
                            href={r.href}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="group flex items-start gap-3 rounded-lg border border-border-subtle bg-surface-subtle p-4 transition-colors hover:border-primary/40 hover:bg-surface-hover dark:border-border-dark-subtle dark:bg-surface-dark-subtle dark:hover:border-primary/60 dark:hover:bg-surface-dark-hover"
                        >
                            <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-md bg-primary/10 text-primary dark:bg-primary/15 dark:text-primary-light">
                                {r.icon}
                            </div>
                            <div className="min-w-0 flex-1">
                                <div className="flex items-center gap-1.5 text-sm font-medium text-text-primary dark:text-text-dark-primary">
                                    {r.title}
                                    <LaunchIcon
                                        sx={{ fontSize: 12 }}
                                        className="opacity-60 group-hover:opacity-100"
                                    />
                                </div>
                                <p className="mt-0.5 text-xs text-text-muted dark:text-text-dark-muted">
                                    {r.description}
                                </p>
                            </div>
                        </Link>
                    ))}
                </div>
            </section>

            <section className="rounded-xl border border-border-subtle bg-surface-subtle px-8 py-10 text-center dark:border-border-dark-subtle dark:bg-surface-dark-subtle">
                <h3 className="text-xl font-semibold tracking-tight text-text-primary dark:text-text-dark-primary">
                    Ready to manage your fleet?
                </h3>
                <p className="mx-auto mt-2 max-w-xl text-sm text-text-muted dark:text-text-dark-muted">
                    Open the dashboard to inspect namespaces, clusters, shards, and nodes — with
                    keyboard-first navigation.
                </p>
                <div className="mt-5">
                    <Button
                        variant="contained"
                        size="large"
                        onClick={() => router.push("/namespaces")}
                        endIcon={<ArrowForwardIcon sx={{ fontSize: 14 }} />}
                    >
                        Open dashboard
                    </Button>
                </div>
            </section>
        </div>
    );
}
