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
    Dialog,
    DialogTitle,
    DialogContent,
    DialogActions,
    Button,
    Typography,
    FormControl,
    FormLabel,
    RadioGroup,
    FormControlLabel,
    Radio,
    Box,
    Chip,
    CircularProgress,
    Alert,
    Snackbar,
    TextField,
    Switch,
    alpha,
    useTheme,
} from "@mui/material";
import MoveUpIcon from "@mui/icons-material/MoveUp";
import StorageIcon from "@mui/icons-material/Storage";
import DeviceHubIcon from "@mui/icons-material/DeviceHub";
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

export const MigrationDialog: React.FC<MigrationDialogProps> = ({
    open,
    onClose,
    namespace,
    cluster,
    shards,
    onSuccess,
}) => {
    const [targetShardIndex, setTargetShardIndex] = useState<number>(-1);
    const [slotNumber, setSlotNumber] = useState<string>("");
    const [slotOnly, setSlotOnly] = useState<boolean>(false);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string>("");
    const theme = useTheme();

    const availableTargetShards = shards.filter((shard) => shard.hasSlots);

    const validateSlotInput = (input: string): { isValid: boolean; error?: string } => {
        if (!input.trim()) {
            return { isValid: false, error: "Please enter a slot number or range" };
        }

        // Check if it's a range (contains dash)
        if (input.includes("-")) {
            const parts = input.split("-");
            if (parts.length !== 2) {
                return {
                    isValid: false,
                    error: "Invalid range format. Use format: start-end (e.g., 100-200)",
                };
            }

            const start = parseInt(parts[0].trim());
            const end = parseInt(parts[1].trim());

            if (isNaN(start) || isNaN(end)) {
                return {
                    isValid: false,
                    error: "Both start and end of range must be valid numbers",
                };
            }

            if (start < 0 || end > 16383 || start > 16383 || end < 0) {
                return { isValid: false, error: "Slot numbers must be between 0 and 16383" };
            }

            if (start > end) {
                return {
                    isValid: false,
                    error: "Start slot must be less than or equal to end slot",
                };
            }

            return { isValid: true };
        } else {
            const slot = parseInt(input.trim());
            if (isNaN(slot) || slot < 0 || slot > 16383) {
                return { isValid: false, error: "Slot number must be between 0 and 16383" };
            }
            return { isValid: true };
        }
    };

    const handleMigration = async () => {
        if (targetShardIndex === -1 || !slotNumber.trim()) {
            setError("Please select a target shard and enter a slot number or range");
            return;
        }

        // Validate slot input
        const validation = validateSlotInput(slotNumber);
        if (!validation.isValid) {
            setError(validation.error || "Invalid slot input");
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

            if (result) {
                setError(result);
            } else {
                onSuccess();
                onClose();
                resetForm();
            }
        } catch (err) {
            setError("An unexpected error occurred during migration");
        } finally {
            setLoading(false);
        }
    };

    const resetForm = () => {
        setTargetShardIndex(-1);
        setSlotNumber("");
        setSlotOnly(false);
        setError("");
    };

    const handleClose = () => {
        if (!loading) {
            onClose();
            resetForm();
        }
    };

    const getSlotRangeDisplay = (slotRanges: string[]) => {
        if (!slotRanges || slotRanges.length === 0) return "No slots";
        return slotRanges.join(", ");
    };

    return (
        <>
            <Dialog
                open={open}
                onClose={handleClose}
                maxWidth="sm"
                fullWidth
                PaperProps={{
                    sx: {
                        borderRadius: "24px",
                        boxShadow: "0 25px 50px -12px rgba(0, 0, 0, 0.25)",
                        backgroundImage:
                            theme.palette.mode === "dark"
                                ? "linear-gradient(to bottom, rgba(66, 66, 66, 0.8), rgba(33, 33, 33, 0.9))"
                                : "linear-gradient(to bottom, #ffffff, #f9fafb)",
                        backdropFilter: "blur(20px)",
                        overflow: "hidden",
                    },
                }}
            >
                <DialogTitle
                    sx={{
                        background:
                            theme.palette.mode === "dark"
                                ? alpha(theme.palette.background.paper, 0.5)
                                : alpha(theme.palette.primary.light, 0.1),
                        borderBottom: `1px solid ${
                            theme.palette.mode === "dark"
                                ? theme.palette.grey[800]
                                : theme.palette.grey[200]
                        }`,
                        padding: "24px",
                    }}
                >
                    <Box display="flex" alignItems="center" gap={2}>
                        <MoveUpIcon className="text-primary" sx={{ fontSize: 28 }} />
                        <Box>
                            <Typography
                                variant="h6"
                                className="font-semibold text-gray-800 dark:text-gray-100"
                            >
                                Migrate Slot
                            </Typography>
                            <Typography
                                variant="body2"
                                className="text-gray-500 dark:text-gray-400"
                            >
                                Move a slot to a different shard
                            </Typography>
                        </Box>
                    </Box>
                </DialogTitle>

                <DialogContent sx={{ padding: "24px" }}>
                    <Box mb={1} mt={1}>
                        <TextField
                            label="Slot or Slot Range"
                            value={slotNumber}
                            onChange={(e) => setSlotNumber(e.target.value)}
                            fullWidth
                            variant="outlined"
                            placeholder="e.g., 123 or 100-200"
                            helperText="Enter a single slot (123) or slot range (100-200). Slots must be between 0 and 16383"
                            sx={{
                                "& .MuiOutlinedInput-root": {
                                    borderRadius: "16px",
                                    "&.Mui-focused": {
                                        boxShadow: `0 0 0 2px ${alpha(theme.palette.primary.main, 0.2)}`,
                                    },
                                },
                            }}
                        />
                    </Box>

                    <Box mb={1}>
                        <FormControlLabel
                            control={
                                <Switch
                                    checked={slotOnly}
                                    onChange={(e) => setSlotOnly(e.target.checked)}
                                    sx={{
                                        "& .MuiSwitch-switchBase.Mui-checked": {
                                            color: theme.palette.primary.main,
                                        },
                                        "& .MuiSwitch-switchBase.Mui-checked + .MuiSwitch-track": {
                                            backgroundColor: theme.palette.primary.main,
                                        },
                                    }}
                                />
                            }
                            label={
                                <Box>
                                    <Typography variant="body1" className="font-medium">
                                        Slot-only migration
                                    </Typography>
                                    <Typography variant="body2" className="text-gray-500">
                                        Migrate only the slot without data
                                    </Typography>
                                </Box>
                            }
                        />
                    </Box>

                    {availableTargetShards.length > 0 ? (
                        <Box>
                            <Typography
                                variant="subtitle1"
                                className="mb-3 font-medium text-gray-700 dark:text-gray-300"
                            >
                                Select Target Shard
                            </Typography>
                            <FormControl component="fieldset" fullWidth>
                                <RadioGroup
                                    value={targetShardIndex.toString()}
                                    onChange={(e) => setTargetShardIndex(parseInt(e.target.value))}
                                >
                                    {availableTargetShards.map((shard) => (
                                        <FormControlLabel
                                            key={shard.index}
                                            value={shard.index.toString()}
                                            control={
                                                <Radio
                                                    sx={{
                                                        color: theme.palette.primary.main,
                                                        "&.Mui-checked": {
                                                            color: theme.palette.primary.main,
                                                        },
                                                    }}
                                                />
                                            }
                                            label={
                                                <Box
                                                    display="flex"
                                                    alignItems="center"
                                                    gap={2}
                                                    flex={1}
                                                >
                                                    <StorageIcon className="text-info" />
                                                    <Box flex={1}>
                                                        <Typography
                                                            variant="body1"
                                                            className="font-medium"
                                                        >
                                                            Shard {shard.index}
                                                        </Typography>
                                                        <Typography
                                                            variant="body2"
                                                            className="text-gray-500"
                                                        >
                                                            Slots:{" "}
                                                            {getSlotRangeDisplay(shard.slotRanges)}
                                                        </Typography>
                                                        <Typography
                                                            variant="body2"
                                                            className="text-gray-500"
                                                        >
                                                            Nodes: {shard.nodeCount}
                                                        </Typography>
                                                    </Box>
                                                    <Box display="flex" gap={1} flexWrap="wrap">
                                                        <Chip
                                                            label={`${shard.nodeCount} nodes`}
                                                            size="small"
                                                            className="bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200"
                                                        />
                                                        {shard.hasMigration && (
                                                            <Chip
                                                                label="Migrating"
                                                                size="small"
                                                                className="bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200"
                                                            />
                                                        )}
                                                        {shard.hasImporting && (
                                                            <Chip
                                                                label="Importing"
                                                                size="small"
                                                                className="bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200"
                                                            />
                                                        )}
                                                    </Box>
                                                </Box>
                                            }
                                            sx={{
                                                p: 2,
                                                m: 0,
                                                mb: 2,
                                                border: `1px solid ${
                                                    targetShardIndex === shard.index
                                                        ? theme.palette.primary.main
                                                        : theme.palette.grey[300]
                                                }`,
                                                borderRadius: "16px",
                                                backgroundColor:
                                                    targetShardIndex === shard.index
                                                        ? alpha(theme.palette.primary.main, 0.1)
                                                        : "transparent",
                                                transition: "all 0.2s ease",
                                                "&:hover": {
                                                    backgroundColor: alpha(
                                                        theme.palette.primary.main,
                                                        0.05
                                                    ),
                                                },
                                            }}
                                        />
                                    ))}
                                </RadioGroup>
                            </FormControl>
                        </Box>
                    ) : (
                        <Alert severity="warning" sx={{ borderRadius: "16px" }}>
                            No target shards available for migration. At least one shard with slots
                            is required.
                        </Alert>
                    )}
                </DialogContent>

                <DialogActions
                    sx={{
                        background:
                            theme.palette.mode === "dark"
                                ? alpha(theme.palette.background.paper, 0.5)
                                : alpha(theme.palette.primary.light, 0.05),
                        borderTop: `1px solid ${
                            theme.palette.mode === "dark"
                                ? theme.palette.grey[800]
                                : theme.palette.grey[200]
                        }`,
                        padding: "24px",
                        justifyContent: "space-between",
                    }}
                >
                    <Button
                        onClick={handleClose}
                        disabled={loading}
                        sx={{
                            textTransform: "none",
                            fontWeight: 500,
                            borderRadius: "16px",
                            px: 3,
                            py: 1,
                        }}
                    >
                        Cancel
                    </Button>
                    <Button
                        onClick={handleMigration}
                        variant="contained"
                        disabled={
                            loading ||
                            availableTargetShards.length === 0 ||
                            targetShardIndex === -1 ||
                            !slotNumber.trim()
                        }
                        startIcon={loading ? <CircularProgress size={16} /> : <MoveUpIcon />}
                        sx={{
                            textTransform: "none",
                            fontWeight: 600,
                            borderRadius: "16px",
                            px: 4,
                            py: 1,
                            backgroundColor: theme.palette.primary.main,
                            "&:hover": {
                                backgroundColor: theme.palette.primary.dark,
                                transform: "translateY(-1px)",
                                boxShadow: "0 6px 15px rgba(0, 0, 0, 0.1)",
                            },
                        }}
                    >
                        {loading ? "Processing..." : "Start Migration"}
                    </Button>
                </DialogActions>
            </Dialog>

            <Snackbar
                open={!!error}
                autoHideDuration={6000}
                onClose={() => setError("")}
                anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
            >
                <Alert
                    onClose={() => setError("")}
                    severity="error"
                    variant="filled"
                    sx={{ borderRadius: "16px" }}
                >
                    {error}
                </Alert>
            </Snackbar>
        </>
    );
};
