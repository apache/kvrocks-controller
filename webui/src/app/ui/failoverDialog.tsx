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
    alpha,
    useTheme,
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
    const [error, setError] = useState<string>("");
    const theme = useTheme();

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

            if (result.error) {
                setError(result.error);
            } else {
                onSuccess();
                onClose();
                setSelectedNodeId("auto");
            }
        } catch (err) {
            setError("An unexpected error occurred during failover");
        } finally {
            setLoading(false);
        }
    };

    const handleClose = () => {
        if (!loading) {
            onClose();
            setSelectedNodeId("auto");
            setError("");
        }
    };

    const truncateId = (id: string, length: number = 8) => {
        return id.length > length ? `${id.substring(0, length)}...` : id;
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
                        <SwapHorizIcon className="text-primary" sx={{ fontSize: 28 }} />
                        <Box>
                            <Typography
                                variant="h6"
                                className="font-semibold text-gray-800 dark:text-gray-100"
                            >
                                Failover Shard Master
                            </Typography>
                            <Typography
                                variant="body2"
                                className="text-gray-500 dark:text-gray-400"
                            >
                                Promote a replica node to master
                            </Typography>
                        </Box>
                    </Box>
                </DialogTitle>

                <DialogContent sx={{ padding: "24px" }}>
                    <Box mb={2}>
                        <Typography
                            variant="subtitle1"
                            className="mb-2 font-medium text-gray-700 dark:text-gray-300"
                        >
                            Current Master
                        </Typography>
                        {masterNode && (
                            <Box
                                sx={{
                                    p: 2,
                                    border: `1px solid ${theme.palette.success.light}`,
                                    borderRadius: "16px",
                                    backgroundColor: alpha(theme.palette.success.light, 0.1),
                                }}
                            >
                                <Box display="flex" alignItems="center" gap={2}>
                                    <CheckCircleIcon className="text-success" />
                                    <Box flex={1}>
                                        <Typography variant="body1" className="font-medium">
                                            {masterNode.addr}
                                        </Typography>
                                        <Typography variant="body2" className="text-gray-500">
                                            ID: {truncateId(masterNode.id)}
                                        </Typography>
                                    </Box>
                                    <Chip
                                        label="Master"
                                        size="small"
                                        className="bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200"
                                    />
                                </Box>
                            </Box>
                        )}
                    </Box>

                    {slaveNodes.length > 0 ? (
                        <Box>
                            <Typography
                                variant="subtitle1"
                                className="mb-3 font-medium text-gray-700 dark:text-gray-300"
                            >
                                Select New Master
                            </Typography>
                            <FormControl component="fieldset" fullWidth>
                                <RadioGroup
                                    value={selectedNodeId}
                                    onChange={(e) => setSelectedNodeId(e.target.value)}
                                >
                                    <FormControlLabel
                                        value="auto"
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
                                            <Box>
                                                <Typography variant="body1" className="font-medium">
                                                    Automatic Selection
                                                </Typography>
                                                <Typography
                                                    variant="body2"
                                                    className="text-gray-500"
                                                >
                                                    Let the controller choose the best replica
                                                </Typography>
                                            </Box>
                                        }
                                        sx={{
                                            p: 2,
                                            m: 0,
                                            mb: 2,
                                            border: `1px solid ${
                                                selectedNodeId === "auto"
                                                    ? theme.palette.primary.main
                                                    : theme.palette.grey[300]
                                            }`,
                                            borderRadius: "16px",
                                            backgroundColor:
                                                selectedNodeId === "auto"
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

                                    {slaveNodes.map((node) => (
                                        <FormControlLabel
                                            key={node.id}
                                            value={node.id}
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
                                                    <DeviceHubIcon className="text-info" />
                                                    <Box flex={1}>
                                                        <Typography
                                                            variant="body1"
                                                            className="font-medium"
                                                        >
                                                            {node.addr}
                                                        </Typography>
                                                        <Typography
                                                            variant="body2"
                                                            className="text-gray-500"
                                                        >
                                                            ID: {truncateId(node.id)}
                                                        </Typography>
                                                    </Box>
                                                    <Chip
                                                        label="Replica"
                                                        size="small"
                                                        className="bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200"
                                                    />
                                                </Box>
                                            }
                                            sx={{
                                                p: 2,
                                                m: 0,
                                                mb: 2,
                                                border: `1px solid ${
                                                    selectedNodeId === node.id
                                                        ? theme.palette.primary.main
                                                        : theme.palette.grey[300]
                                                }`,
                                                borderRadius: "16px",
                                                backgroundColor:
                                                    selectedNodeId === node.id
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
                            No replica nodes available for failover. At least one replica node is
                            required.
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
                        onClick={handleFailover}
                        variant="contained"
                        disabled={loading || slaveNodes.length === 0}
                        startIcon={loading ? <CircularProgress size={16} /> : <SwapHorizIcon />}
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
                        {loading ? "Processing..." : "Start Failover"}
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
