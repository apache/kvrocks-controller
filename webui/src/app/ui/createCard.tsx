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
            <Paper
                elevation={0}
                className={`card w-72 h-52 transition-all ${className}`}
            >
                {children}
            </Paper>
        </Box>
    );
};

export const AddClusterCard = ({ namespace }: { namespace: string }) => {
    return (
        <CreateCard className="bg-gradient-to-br from-primary-light/5 to-primary/10 dark:from-primary-dark/10 dark:to-primary/20 flex items-center justify-center">
            <div className="text-center">
                <FontAwesomeIcon
                    icon={faCirclePlus}
                    size="4x"
                    className="text-primary/40 dark:text-primary-light/40 mb-4"
                />
                <div className="flex flex-row items-center justify-center space-x-2 mt-2">
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

export const AddShardCard = ({
    namespace,
    cluster,
}: {
  namespace: string;
  cluster: string;
}) => {
    return (
        <CreateCard className="bg-gradient-to-br from-primary-light/5 to-primary/10 dark:from-primary-dark/10 dark:to-primary/20 flex items-center justify-center">
            <div className="text-center">
                <FontAwesomeIcon
                    icon={faCirclePlus}
                    size="4x"
                    className="text-primary/40 dark:text-primary-light/40 mb-6"
                />
                <div className="flex flex-row items-center justify-center space-x-3 mt-4">
                    <ShardCreation
                        position="card"
                        namespace={namespace}
                        cluster={cluster}
                    />
                    <MigrateSlot
                        position="card"
                        namespace={namespace}
                        cluster={cluster}
                    />
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
        <CreateCard className="bg-gradient-to-br from-primary-light/5 to-primary/10 dark:from-primary-dark/10 dark:to-primary/20 flex items-center justify-center">
            <div className="text-center">
                <FontAwesomeIcon
                    icon={faCirclePlus}
                    size="4x"
                    className="text-primary/40 dark:text-primary-light/40 mb-6"
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
    children 
}: { 
    title: string; 
    description?: string;
    tags?: Array<{label: string, color?: string}>;
    children: ReactNode; 
}) => {
    return (
        <CreateCard>
            <div className="flex flex-col h-full">
                <div className="font-medium text-lg mb-1">{title}</div>
                {description && (
                    <div className="text-sm text-gray-500 dark:text-gray-400 mb-3">
                        {description}
                    </div>
                )}
                <div className="flex-grow">
                    {children}
                </div>
                {tags && tags.length > 0 && (
                    <div className="flex flex-wrap gap-1 mt-3">
                        {tags.map((tag, i) => (
                            <Chip 
                                key={i} 
                                label={tag.label} 
                                size="small" 
                                color={tag.color as any || "default"}
                                className="text-xs"
                            />
                        ))}
                    </div>
                )}
            </div>
        </CreateCard>
    );
};
