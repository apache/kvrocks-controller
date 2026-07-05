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

import React, { useState } from "react";
import {
    Alert,
    Button,
    Chip,
    CircularProgress,
    Dialog,
    DialogActions,
    DialogContent,
    DialogTitle,
    Snackbar,
} from "@mui/material";
import SwapHorizIcon from "@mui/icons-material/SwapHoriz";
import CheckCircleIcon from "@mui/icons-material/CheckCircle";
import DeviceHubIcon from "@mui/icons-material/DeviceHub";
import { failoverShard } from "@/app/lib/api";

interface Node {
    id: string;
    addr: string;
    role: string;
    created_at: number;
}

interface FailoverDialogProps {
    open: boolean;
    onClose: () => void;
    namespace: string;
    cluster: string;
    shard: string;
    nodes: Node[];
    onSuccess: () => void;
}

const truncateId = (id: string, length = 8) =>
    id.length > length ? `${id.substring(0, length)}…` : id;

const RadioRow = ({
    checked,
    onChange,
    children,
}: {
    checked: boolean;
    onChange: () => void;
    children: React.ReactNode;
}) => (
    <label
        onClick={onChange}
        className={`flex cursor-pointer items-center gap-3 rounded-md border px-3 py-2 transition-colors ${
            checked
                ? "border-primary bg-primary/5 dark:border-primary/70 dark:bg-primary/10"
                : "border-border-subtle hover:border-border-strong dark:border-border-dark-subtle dark:hover:border-border-dark-strong"
        }`}
    >
        <span
            className={`flex h-3.5 w-3.5 items-center justify-center rounded-full border ${
                checked
                    ? "border-primary bg-primary"
                    : "border-border-strong dark:border-border-dark-strong"
            }`}
        >
            {checked && <span className="h-1.5 w-1.5 rounded-full bg-white" />}
        </span>
        <div className="min-w-0 flex-1">{children}</div>
    </label>
);

export const FailoverDialog: React.FC<FailoverDialogProps> = ({
    open,
    onClose,
    namespace,
    cluster,
    shard,
    nodes,
    onSuccess,
}) => {
    const [selectedNodeId, setSelectedNodeId] = useState<string>("auto");
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState("");

    const masterNode = nodes.find((node) => node.role === "master");
    const slaveNodes = nodes.filter((node) => node.role === "slave");

    const handleFailover = async () => {
        setLoading(true);
        setError("");
        try {
            const result = await failoverShard(
                namespace,
                cluster,
                shard,
                selectedNodeId === "auto" ? undefined : selectedNodeId
            );
            if (result.error) setError(result.error);
            else {
                onSuccess();
                onClose();
                setSelectedNodeId("auto");
            }
        } catch {
            setError("An unexpected error occurred during failover");
        } finally {
            setLoading(false);
        }
    };

    const handleClose = () => {
        if (loading) return;
        onClose();
        setSelectedNodeId("auto");
        setError("");
    };

    return (
        <>
            <Dialog open={open} onClose={handleClose} maxWidth="sm" fullWidth>
                <DialogTitle>
                    <span className="flex items-center gap-2">
                        <SwapHorizIcon sx={{ fontSize: 16 }} className="text-primary" />
                        <span>Failover shard master</span>
                    </span>
                </DialogTitle>

                <DialogContent>
                    <p className="mb-4 text-xs text-text-muted dark:text-text-dark-muted">
                        Promote a replica to master. The current master will become a replica once
                        the operation completes.
                    </p>

                    {masterNode && (
                        <div className="mb-4">
                            <div className="lin-eyebrow mb-1.5">Current master</div>
                            <div className="flex items-center gap-3 rounded-md border border-success/30 bg-success/5 px-3 py-2">
                                <CheckCircleIcon
                                    sx={{ fontSize: 14 }}
                                    className="text-success"
                                />
                                <div className="min-w-0 flex-1">
                                    <div className="truncate text-sm font-medium text-text-primary dark:text-text-dark-primary">
                                        {masterNode.addr}
                                    </div>
                                    <div className="text-xs text-text-muted dark:text-text-dark-muted">
                                        ID: {truncateId(masterNode.id)}
                                    </div>
                                </div>
                                <Chip label="Master" size="small" />
                            </div>
                        </div>
                    )}

                    {slaveNodes.length > 0 ? (
                        <div>
                            <div className="lin-eyebrow mb-1.5">Select new master</div>
                            <div className="space-y-1.5">
                                <RadioRow
                                    checked={selectedNodeId === "auto"}
                                    onChange={() => setSelectedNodeId("auto")}
                                >
                                    <div className="text-sm font-medium text-text-primary dark:text-text-dark-primary">
                                        Automatic
                                    </div>
                                    <div className="text-xs text-text-muted dark:text-text-dark-muted">
                                        Controller picks the best replica
                                    </div>
                                </RadioRow>

                                {slaveNodes.map((node) => (
                                    <RadioRow
                                        key={node.id}
                                        checked={selectedNodeId === node.id}
                                        onChange={() => setSelectedNodeId(node.id)}
                                    >
                                        <div className="flex items-center gap-2">
                                            <DeviceHubIcon
                                                sx={{ fontSize: 14 }}
                                                className="text-info"
                                            />
                                            <div className="min-w-0 flex-1">
                                                <div className="truncate text-sm font-medium text-text-primary dark:text-text-dark-primary">
                                                    {node.addr}
                                                </div>
                                                <div className="text-xs text-text-muted dark:text-text-dark-muted">
                                                    ID: {truncateId(node.id)}
                                                </div>
                                            </div>
                                            <Chip label="Replica" size="small" />
                                        </div>
                                    </RadioRow>
                                ))}
                            </div>
                        </div>
                    ) : (
                        <Alert severity="warning" variant="outlined">
                            No replica nodes available. At least one replica is required for a
                            manual failover.
                        </Alert>
                    )}
                </DialogContent>

                <DialogActions>
                    <Button
                        onClick={handleClose}
                        disabled={loading}
                        variant="outlined"
                        size="small"
                    >
                        Cancel
                    </Button>
                    <Button
                        onClick={handleFailover}
                        variant="contained"
                        size="small"
                        disabled={loading || slaveNodes.length === 0}
                        startIcon={
                            loading ? (
                                <CircularProgress size={12} color="inherit" />
                            ) : (
                                <SwapHorizIcon sx={{ fontSize: 13 }} />
                            )
                        }
                    >
                        {loading ? "Working…" : "Start failover"}
                    </Button>
                </DialogActions>
            </Dialog>

            <Snackbar
                open={!!error}
                autoHideDuration={6000}
                onClose={() => setError("")}
                anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
            >
                <Alert onClose={() => setError("")} severity="error" variant="filled">
                    {error}
                </Alert>
            </Snackbar>
        </>
    );
};
