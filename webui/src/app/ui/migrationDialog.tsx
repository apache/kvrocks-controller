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

import React, { useEffect, useMemo, useState } from "react";
import {
    Alert,
    Button,
    Chip,
    CircularProgress,
    Dialog,
    DialogActions,
    DialogContent,
    DialogTitle,
    FormControlLabel,
    Snackbar,
    Switch,
    TextField,
} from "@mui/material";
import MoveUpIcon from "@mui/icons-material/MoveUp";
import StorageIcon from "@mui/icons-material/Storage";
import { migrateSlot } from "@/app/lib/api";

interface Shard {
    index: number;
    nodes: any[];
    slotRanges: string[];
    migratingSlot: string;
    importingSlot: string;
    targetShardIndex: number;
    nodeCount: number;
    hasSlots: boolean;
    hasMigration: boolean;
    hasImporting: boolean;
}

interface MigrationDialogProps {
    open: boolean;
    onClose: () => void;
    namespace: string;
    cluster: string;
    shards: Shard[];
    onSuccess: () => void;
}

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

// Given the user's slot input, find which shard currently owns that slot.
// Any slot inside a range is enough to identify the source, so we peek at the
// first slot for ranges. Returns -1 when the slot is unassigned or invalid.
const findSourceShardIndex = (input: string, shards: Shard[]): number => {
    const trimmed = input.trim();
    if (!trimmed) return -1;
    const first = trimmed.includes("-") ? trimmed.split("-")[0] : trimmed;
    const slot = parseInt(first.trim());
    if (isNaN(slot)) return -1;
    for (const shard of shards) {
        for (const range of shard.slotRanges) {
            let start: number;
            let end: number;
            if (range.includes("-")) {
                [start, end] = range.split("-").map(Number);
            } else {
                start = end = Number(range);
            }
            if (!isNaN(start) && !isNaN(end) && slot >= start && slot <= end) {
                return shard.index;
            }
        }
    }
    return -1;
};

const validateSlotInput = (input: string): { isValid: boolean; error?: string } => {
    if (!input.trim()) return { isValid: false, error: "Enter a slot number or range" };

    if (input.includes("-")) {
        const parts = input.split("-");
        if (parts.length !== 2) {
            return { isValid: false, error: "Range format: start-end (e.g., 100-200)" };
        }
        const start = parseInt(parts[0].trim());
        const end = parseInt(parts[1].trim());
        if (isNaN(start) || isNaN(end)) {
            return { isValid: false, error: "Range values must be valid numbers" };
        }
        if (start < 0 || end > 16383 || start > 16383 || end < 0) {
            return { isValid: false, error: "Slots must be 0 to 16383" };
        }
        if (start > end) {
            return { isValid: false, error: "Start must be ≤ end" };
        }
        return { isValid: true };
    }

    const slot = parseInt(input.trim());
    if (isNaN(slot) || slot < 0 || slot > 16383) {
        return { isValid: false, error: "Slot must be 0 to 16383" };
    }
    return { isValid: true };
};

export const MigrationDialog: React.FC<MigrationDialogProps> = ({
    open,
    onClose,
    namespace,
    cluster,
    shards,
    onSuccess,
}) => {
    const [targetShardIndex, setTargetShardIndex] = useState<number>(-1);
    const [slotNumber, setSlotNumber] = useState("");
    const [slotOnly, setSlotOnly] = useState(false);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState("");

    const sourceShardIndex = useMemo(
        () => findSourceShardIndex(slotNumber, shards),
        [slotNumber, shards]
    );

    const availableTargetShards = useMemo(
        () => shards.filter((s) => s.index !== sourceShardIndex),
        [shards, sourceShardIndex]
    );

    // Clear the selection if the user typed a slot that maps to the currently
    // selected target — the source can never be its own target.
    useEffect(() => {
        if (targetShardIndex !== -1 && targetShardIndex === sourceShardIndex) {
            setTargetShardIndex(-1);
        }
    }, [sourceShardIndex, targetShardIndex]);

    const resetForm = () => {
        setTargetShardIndex(-1);
        setSlotNumber("");
        setSlotOnly(false);
        setError("");
    };

    const handleClose = () => {
        if (loading) return;
        onClose();
        resetForm();
    };

    const handleMigration = async () => {
        if (targetShardIndex === -1 || !slotNumber.trim()) {
            setError("Select a target shard and enter a slot");
            return;
        }
        const validation = validateSlotInput(slotNumber);
        if (!validation.isValid) {
            setError(validation.error || "Invalid slot");
            return;
        }
        setLoading(true);
        setError("");
        try {
            const result = await migrateSlot(
                namespace,
                cluster,
                targetShardIndex,
                slotNumber.trim(),
                slotOnly
            );
            if (result) setError(result);
            else {
                onSuccess();
                onClose();
                resetForm();
            }
        } catch {
            setError("An unexpected error occurred during migration");
        } finally {
            setLoading(false);
        }
    };

    return (
        <>
            <Dialog open={open} onClose={handleClose} maxWidth="sm" fullWidth>
                <DialogTitle>
                    <span className="flex items-center gap-2">
                        <MoveUpIcon sx={{ fontSize: 16 }} className="text-primary" />
                        <span>Migrate slot</span>
                    </span>
                </DialogTitle>

                <DialogContent>
                    <p className="mb-4 text-xs text-text-muted dark:text-text-dark-muted">
                        Move a slot or slot range between shards. Slots range from 0 to 16383.
                    </p>

                    <div className="mb-4">
                        <TextField
                            label="Slot or range"
                            value={slotNumber}
                            onChange={(e) => setSlotNumber(e.target.value)}
                            fullWidth
                            size="small"
                            placeholder="e.g. 123 or 100-200"
                            helperText="Single slot (123) or range (100-200)"
                        />
                    </div>

                    <div className="mb-4">
                        <FormControlLabel
                            control={
                                <Switch
                                    checked={slotOnly}
                                    onChange={(e) => setSlotOnly(e.target.checked)}
                                    size="small"
                                />
                            }
                            label={
                                <span className="text-sm">
                                    <span className="text-text-primary dark:text-text-dark-primary">
                                        Slot-only migration
                                    </span>
                                    <span className="ml-2 text-xs text-text-muted dark:text-text-dark-muted">
                                        Move the slot without data
                                    </span>
                                </span>
                            }
                        />
                    </div>

                    {availableTargetShards.length > 0 ? (
                        <div>
                            <div className="mb-1.5 flex items-center justify-between gap-2">
                                <span className="lin-eyebrow">Select target shard</span>
                                {sourceShardIndex !== -1 && (
                                    <span className="text-2xs text-text-muted dark:text-text-dark-muted">
                                        Source: Shard {sourceShardIndex + 1} (excluded)
                                    </span>
                                )}
                            </div>
                            <div className="space-y-1.5">
                                {availableTargetShards.map((shard) => (
                                    <RadioRow
                                        key={shard.index}
                                        checked={targetShardIndex === shard.index}
                                        onChange={() => setTargetShardIndex(shard.index)}
                                    >
                                        <div className="flex items-center gap-3">
                                            <StorageIcon
                                                sx={{ fontSize: 14 }}
                                                className="text-info"
                                            />
                                            <div className="min-w-0 flex-1">
                                                <div className="text-sm font-medium text-text-primary dark:text-text-dark-primary">
                                                    Shard {shard.index + 1}
                                                </div>
                                                <div className="truncate text-xs text-text-muted dark:text-text-dark-muted">
                                                    Slots:{" "}
                                                    {shard.slotRanges.length
                                                        ? shard.slotRanges.join(", ")
                                                        : "—"}{" "}
                                                    · {shard.nodeCount} nodes
                                                </div>
                                            </div>
                                            <div className="flex shrink-0 gap-1">
                                                {shard.hasMigration && (
                                                    <Chip label="Migrating" size="small" />
                                                )}
                                                {shard.hasImporting && (
                                                    <Chip label="Importing" size="small" />
                                                )}
                                            </div>
                                        </div>
                                    </RadioRow>
                                ))}
                            </div>
                        </div>
                    ) : (
                        <Alert severity="warning" variant="outlined">
                            {sourceShardIndex !== -1
                                ? `Slot ${slotNumber.trim()} lives on Shard ${sourceShardIndex + 1}, and there are no other shards to migrate it to.`
                                : "No target shards available. Add another shard to migrate slots."}
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
                        onClick={handleMigration}
                        variant="contained"
                        size="small"
                        disabled={
                            loading ||
                            availableTargetShards.length === 0 ||
                            targetShardIndex === -1 ||
                            !slotNumber.trim()
                        }
                        startIcon={
                            loading ? (
                                <CircularProgress size={12} color="inherit" />
                            ) : (
                                <MoveUpIcon sx={{ fontSize: 13 }} />
                            )
                        }
                    >
                        {loading ? "Working…" : "Start migration"}
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
