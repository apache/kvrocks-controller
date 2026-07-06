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

import { use, useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { Alert, IconButton, Tooltip } from "@mui/material";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import CheckIcon from "@mui/icons-material/Check";
import DeviceHubIcon from "@mui/icons-material/DeviceHub";

import { listNodes } from "@/app/lib/api";
import { NodeSidebar } from "@/app/ui/sidebar";
import { LoadingSpinner } from "@/app/ui/loadingSpinner";
import { PageHeader, PageShell } from "@/app/ui/pageChrome";

function CopyableField({ label, value, mono }: { label: string; value: string; mono?: boolean }) {
    const [copied, setCopied] = useState(false);
    const copy = () => {
        navigator.clipboard.writeText(value);
        setCopied(true);
        setTimeout(() => setCopied(false), 1500);
    };
    return (
        <div>
            <div className="lin-eyebrow mb-2">{label}</div>
            <div className="flex items-center gap-2">
                <div
                    className={`flex-1 truncate rounded-lg border border-border-subtle bg-surface-subtle px-3 py-2 text-sm text-text-primary dark:border-border-dark-subtle dark:bg-surface-dark-muted dark:text-text-dark-primary ${
                        mono ? "font-mono" : ""
                    }`}
                >
                    {value || "—"}
                </div>
                <Tooltip title={copied ? "Copied" : "Copy"} arrow>
                    <span>
                        <IconButton
                            size="small"
                            onClick={copy}
                            disabled={!value}
                            sx={{ width: 32, height: 32 }}
                            aria-label={`Copy ${label}`}
                        >
                            {copied ? (
                                <CheckIcon sx={{ fontSize: 16 }} className="text-success" />
                            ) : (
                                <ContentCopyIcon sx={{ fontSize: 16 }} />
                            )}
                        </IconButton>
                    </span>
                </Tooltip>
            </div>
        </div>
    );
}

function InfoField({ label, value }: { label: string; value: string }) {
    return (
        <div>
            <div className="lin-eyebrow mb-2">{label}</div>
            <div className="rounded-lg border border-border-subtle bg-surface-subtle px-3 py-2 text-sm text-text-primary dark:border-border-dark-subtle dark:bg-surface-dark-muted dark:text-text-dark-primary">
                {value}
            </div>
        </div>
    );
}

export default function NodePage(props: {
    params: Promise<{ namespace: string; cluster: string; shard: string; node: string }>;
}) {
    const params = use(props.params);
    const { namespace, cluster, shard, node } = params;
    const router = useRouter();
    const [nodes, setNodes] = useState<any[]>([]);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        (async () => {
            try {
                const fetched = await listNodes(namespace, cluster, shard);
                if (!fetched) {
                    router.push("/404");
                    return;
                }
                setNodes(fetched as any[]);
            } catch (error) {
                console.error("Error fetching nodes:", error);
            } finally {
                setLoading(false);
            }
        })();
    }, [namespace, cluster, shard, router]);

    if (loading) return <LoadingSpinner />;

    const current = nodes[parseInt(node)];
    if (!current) {
        return (
            <PageShell
                sidebar={<NodeSidebar namespace={namespace} cluster={cluster} shard={shard} />}
            >
                <div className="p-8">
                    <Alert severity="error" variant="outlined">
                        Node not found.
                    </Alert>
                </div>
            </PageShell>
        );
    }

    const roleBadge =
        current.role === "master" ? (
            <span className="flex items-center gap-1 rounded-md border border-success/40 bg-success/10 px-1.5 py-0.5 text-2xs font-medium text-success">
                <span className="h-1 w-1 rounded-full bg-success" />
                Master
            </span>
        ) : (
            <span className="flex items-center gap-1 rounded-md border border-info/40 bg-info/10 px-1.5 py-0.5 text-2xs font-medium text-info">
                <span className="h-1 w-1 rounded-full bg-info" />
                Replica
            </span>
        );

    return (
        <PageShell sidebar={<NodeSidebar namespace={namespace} cluster={cluster} shard={shard} />}>
            <PageHeader
                icon={<DeviceHubIcon sx={{ fontSize: 20 }} />}
                title={
                    <span className="flex items-center gap-2.5">
                        Node {parseInt(node) + 1}
                        {roleBadge}
                    </span>
                }
                subtitle={`Shard ${parseInt(shard) + 1} · ${cluster} · ${namespace}`}
            />

            <div className="px-8 py-8 md:px-10 md:py-10">
                <div className="grid gap-6 md:grid-cols-2">
                    <section className="rounded-xl border border-border-subtle bg-surface-base p-6 shadow-subtle dark:border-border-dark-subtle dark:bg-surface-dark-subtle">
                        <h2 className="mb-5 text-base font-semibold text-text-primary dark:text-text-dark-primary">
                            Configuration
                        </h2>
                        <div className="space-y-4">
                            <CopyableField label="Node ID" value={current.id} mono />
                            <CopyableField label="Address" value={current.addr} />
                            <InfoField label="Role" value={current.role} />
                            <InfoField
                                label="Created at"
                                value={new Date(current.created_at * 1000).toLocaleString()}
                            />
                            {current.password && (
                                <CopyableField
                                    label="Authentication"
                                    value={current.password}
                                    mono
                                />
                            )}
                        </div>
                    </section>

                    <section className="rounded-xl border border-border-subtle bg-surface-base p-6 shadow-subtle dark:border-border-dark-subtle dark:bg-surface-dark-subtle">
                        <h2 className="mb-5 text-base font-semibold text-text-primary dark:text-text-dark-primary">
                            Location
                        </h2>
                        <div className="space-y-4">
                            <InfoField label="Namespace" value={namespace} />
                            <InfoField label="Cluster" value={cluster} />
                            <InfoField label="Shard" value={`Shard ${parseInt(shard) + 1}`} />
                        </div>
                    </section>
                </div>
            </div>
        </PageShell>
    );
}
