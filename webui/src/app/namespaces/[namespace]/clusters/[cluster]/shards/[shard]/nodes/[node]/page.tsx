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
import {
    Box,
    Typography,
    Chip,
    Paper,
    Divider,
    Grid,
    Alert,
} from "@mui/material";
import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { LoadingSpinner } from "@/app/ui/loadingSpinner";
import { truncateText } from "@/app/utils";
import DeviceHubIcon from '@mui/icons-material/DeviceHub';
import LockIcon from '@mui/icons-material/Lock';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import AccessTimeIcon from '@mui/icons-material/AccessTime';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import StorageIcon from '@mui/icons-material/Storage';
import DnsIcon from '@mui/icons-material/Dns';

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
                <Box className="flex-1 container-inner flex items-center justify-center">
                    <Alert severity="error" variant="filled" className="shadow-lg">
                        Node not found
                    </Alert>
                </Box>
            </div>
        );
    }

    // Get role color and text style
    const getRoleStyles = (role: string) => {
        if (role === 'master') {
            return { 
                color: 'success', 
                textClass: 'text-success font-medium',
                icon: <CheckCircleIcon fontSize="small" className="mr-1" />
            };
        }
        return { 
            color: 'info', 
            textClass: 'text-info font-medium',
            icon: <DeviceHubIcon fontSize="small" className="mr-1" /> 
        };
    };

    const copyToClipboard = (text: string, type: string) => {
        navigator.clipboard.writeText(text);
        setCopied(type);
        setTimeout(() => setCopied(null), 2000);
    };

    const formattedDate = new Date(currentNode.created_at * 1000).toLocaleString();

    return (
        <div className="flex h-full">
            <NodeSidebar namespace={namespace} cluster={cluster} shard={shard} />
            <div className="flex-1 overflow-auto">
                <Box className="container-inner">
                    <Box className="flex items-center justify-between mb-6">
                        <div>
                            <Typography variant="h5" className="font-medium text-gray-800 dark:text-gray-100 flex items-center">
                                <DeviceHubIcon className="mr-2 text-primary dark:text-primary-light" /> 
                                Node {parseInt(node) + 1}
                                <Chip 
                                    label={currentNode.role} 
                                    size="small" 
                                    color={getRoleStyles(currentNode.role).color as any}
                                    className="ml-3"
                                    icon={getRoleStyles(currentNode.role).icon}
                                />
                            </Typography>
                            <Typography variant="body2" className="text-gray-500 dark:text-gray-400 mt-1">
                                Shard {parseInt(shard) + 1}, {cluster} cluster, {namespace} namespace
                            </Typography>
                        </div>
                    </Box>

                    <Paper className="bg-white dark:bg-dark-paper border border-light-border dark:border-dark-border rounded-lg shadow-card p-6 mb-6">
                        <Typography variant="h6" className="mb-4 font-medium flex items-center">
                            <StorageIcon fontSize="small" className="mr-2" />
                            Node Details
                        </Typography>
                        <Divider className="mb-4" />
                        
                        <Grid container spacing={3}>
                            <Grid item xs={12} md={6}>
                                <div className="space-y-4">
                                    <div>
                                        <Typography variant="subtitle2" className="text-gray-500 dark:text-gray-400 mb-1">
                                            ID
                                        </Typography>
                                        <div className="flex items-center">
                                            <Typography variant="body1" className="font-mono bg-gray-50 dark:bg-dark-border px-3 py-2 rounded flex-1 overflow-hidden text-ellipsis">
                                                {currentNode.id}
                                            </Typography>
                                            <IconButton 
                                                onClick={() => copyToClipboard(currentNode.id, 'id')} 
                                                className="ml-2 text-gray-500 hover:text-primary"
                                                title="Copy ID"
                                            >
                                                {copied === 'id' ? 
                                                    <CheckCircleIcon fontSize="small" className="text-success" /> : 
                                                    <ContentCopyIcon fontSize="small" />
                                                }
                                            </IconButton>
                                        </div>
                                    </div>
                                    
                                    <div>
                                        <Typography variant="subtitle2" className="text-gray-500 dark:text-gray-400 mb-1">
                                            Address
                                        </Typography>
                                        <div className="flex items-center">
                                            <Typography variant="body1" className="bg-gray-50 dark:bg-dark-border px-3 py-2 rounded flex-1">
                                                {currentNode.addr}
                                            </Typography>
                                            <IconButton 
                                                onClick={() => copyToClipboard(currentNode.addr, 'addr')} 
                                                className="ml-2 text-gray-500 hover:text-primary"
                                                title="Copy Address"
                                            >
                                                {copied === 'addr' ? 
                                                    <CheckCircleIcon fontSize="small" className="text-success" /> : 
                                                    <ContentCopyIcon fontSize="small" />
                                                }
                                            </IconButton>
                                        </div>
                                    </div>
                                </div>
                            </Grid>
                            
                            <Grid item xs={12} md={6}>
                                <div className="space-y-4">
                                    <div>
                                        <Typography variant="subtitle2" className="text-gray-500 dark:text-gray-400 mb-1">
                                            Role
                                        </Typography>
                                        <Typography variant="body1" className={`${getRoleStyles(currentNode.role).textClass} flex items-center`}>
                                            {getRoleStyles(currentNode.role).icon} {currentNode.role}
                                        </Typography>
                                    </div>
                                    
                                    <div>
                                        <Typography variant="subtitle2" className="text-gray-500 dark:text-gray-400 mb-1">
                                            Created At
                                        </Typography>
                                        <Typography variant="body1" className="flex items-center">
                                            <AccessTimeIcon fontSize="small" className="mr-1 text-gray-500" /> 
                                            {formattedDate}
                                        </Typography>
                                    </div>
                                    
                                    {currentNode.password && (
                                        <div>
                                            <Typography variant="subtitle2" className="text-gray-500 dark:text-gray-400 mb-1">
                                                Authentication
                                            </Typography>
                                            <div className="flex items-center">
                                                <Typography variant="body2" className="bg-gray-50 dark:bg-dark-border px-3 py-2 rounded flex-1 font-mono">
                                                    {currentNode.password ? '••••••••' : 'No password set'}
                                                </Typography>
                                                <IconButton 
                                                    onClick={() => copyToClipboard(currentNode.password, 'pwd')} 
                                                    className="ml-2 text-gray-500 hover:text-primary"
                                                    title="Copy Password"
                                                    disabled={!currentNode.password}
                                                >
                                                    {copied === 'pwd' ? 
                                                        <CheckCircleIcon fontSize="small" className="text-success" /> : 
                                                        <LockIcon fontSize="small" />
                                                    }
                                                </IconButton>
                                            </div>
                                        </div>
                                    )}
                                </div>
                            </Grid>
                        </Grid>
                    </Paper>

                    <Paper className="bg-white dark:bg-dark-paper border border-light-border dark:border-dark-border rounded-lg shadow-card p-6">
                        <Typography variant="h6" className="mb-4 font-medium flex items-center">
                            <DnsIcon fontSize="small" className="mr-2" />
                            Shard Information
                        </Typography>
                        <Divider className="mb-4" />
                        
                        <Grid container spacing={3}>
                            <Grid item xs={12} md={4}>
                                <Typography variant="subtitle2" className="text-gray-500 dark:text-gray-400 mb-1">
                                    Shard
                                </Typography>
                                <Typography variant="body1">
                                    Shard {parseInt(shard) + 1}
                                </Typography>
                            </Grid>
                            <Grid item xs={12} md={4}>
                                <Typography variant="subtitle2" className="text-gray-500 dark:text-gray-400 mb-1">
                                    Cluster
                                </Typography>
                                <Typography variant="body1">
                                    {cluster}
                                </Typography>
                            </Grid>
                            <Grid item xs={12} md={4}>
                                <Typography variant="subtitle2" className="text-gray-500 dark:text-gray-400 mb-1">
                                    Namespace
                                </Typography>
                                <Typography variant="body1">
                                    {namespace}
                                </Typography>
                            </Grid>
                        </Grid>
                    </Paper>
                </Box>
            </div>
        </div>
    );
}

interface IconButtonProps {
    onClick: () => void;
    className?: string;
    title?: string;
    disabled?: boolean;
    children: React.ReactNode;
}

// Custom IconButton component
const IconButton: React.FC<IconButtonProps> = ({ onClick, className = "", title, disabled = false, children }) => {
    return (
        <button 
            onClick={onClick}
            disabled={disabled}
            className={`w-8 h-8 rounded-full flex items-center justify-center hover:bg-gray-100 dark:hover:bg-dark-border ${disabled ? 'opacity-50 cursor-not-allowed' : ''} ${className}`}
            title={title}
        >
            {children}
        </button>
    );
};
