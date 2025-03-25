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

import { Box, Paper, Chip, Tooltip } from "@mui/material";
import React, { ReactNode } from "react";
import {
    ClusterCreation,
    ImportCluster,
    MigrateSlot,
    NodeCreation,
    ShardCreation,
} from "./formCreation";
import { faCirclePlus } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";

interface CreateCardProps {
    children: ReactNode;
    className?: string;
}

export const CreateCard: React.FC<CreateCardProps> = ({ children, className = "" }) => {
    return (
        <Box className="p-3">
            <Paper elevation={0} className={`card h-52 w-72 transition-all ${className}`}>
                {children}
            </Paper>
        </Box>
    );
};

export const AddClusterCard = ({ namespace }: { namespace: string }) => {
    return (
        <CreateCard className="flex items-center justify-center bg-gradient-to-br from-primary-light/5 to-primary/10 dark:from-primary-dark/10 dark:to-primary/20">
            <div className="text-center">
                <FontAwesomeIcon
                    icon={faCirclePlus}
                    size="4x"
                    className="mb-4 text-primary/40 dark:text-primary-light/40"
                />
                <div className="mt-2 flex flex-row items-center justify-center space-x-2">
                    <div className="text-sm leading-tight">
                        <ClusterCreation position="card" namespace={namespace} />
                    </div>
                    <div className="text-sm leading-tight">
                        <ImportCluster position="card" namespace={namespace} />
                    </div>
                </div>
            </div>
        </CreateCard>
    );
};

export const AddShardCard = ({ namespace, cluster }: { namespace: string; cluster: string }) => {
    return (
        <CreateCard className="flex items-center justify-center bg-gradient-to-br from-primary-light/5 to-primary/10 dark:from-primary-dark/10 dark:to-primary/20">
            <div className="text-center">
                <FontAwesomeIcon
                    icon={faCirclePlus}
                    size="4x"
                    className="mb-6 text-primary/40 dark:text-primary-light/40"
                />
                <div className="mt-4 flex flex-row items-center justify-center space-x-3">
                    <ShardCreation position="card" namespace={namespace} cluster={cluster} />
                    <MigrateSlot position="card" namespace={namespace} cluster={cluster} />
                </div>
            </div>
        </CreateCard>
    );
};

export const AddNodeCard = ({
    namespace,
    cluster,
    shard,
}: {
    namespace: string;
    cluster: string;
    shard: string;
}) => {
    return (
        <CreateCard className="flex items-center justify-center bg-gradient-to-br from-primary-light/5 to-primary/10 dark:from-primary-dark/10 dark:to-primary/20">
            <div className="text-center">
                <FontAwesomeIcon
                    icon={faCirclePlus}
                    size="4x"
                    className="mb-6 text-primary/40 dark:text-primary-light/40"
                />
                <div className="mt-4">
                    <NodeCreation
                        position="card"
                        namespace={namespace}
                        cluster={cluster}
                        shard={shard}
                    />
                </div>
            </div>
        </CreateCard>
    );
};

export const ResourceCard = ({
    title,
    description,
    tags,
    children,
}: {
    title: string;
    description?: string;
    tags?: Array<{ label: string; color?: string }>;
    children: ReactNode;
}) => {
    return (
        <CreateCard>
            <div className="flex h-full flex-col">
                <div className="mb-1 text-lg font-medium">{title}</div>
                {description && (
                    <div className="mb-3 text-sm text-gray-500 dark:text-gray-400">
                        {description}
                    </div>
                )}
                <div className="flex-grow">{children}</div>
                {tags && tags.length > 0 && (
                    <div className="mt-3 flex flex-wrap gap-1">
                        {tags.map((tag, i) => (
                            <Chip
                                key={i}
                                label={tag.label}
                                size="small"
                                color={(tag.color as any) || "default"}
                                className="text-xs"
                            />
                        ))}
                    </div>
                )}
            </div>
        </CreateCard>
    );
};
