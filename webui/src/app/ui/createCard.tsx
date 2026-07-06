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

import { Chip } from "@mui/material";
import React, { ReactNode } from "react";
import AddIcon from "@mui/icons-material/Add";
import {
    ClusterCreation,
    ImportCluster,
    MigrateSlot,
    NodeCreation,
    ShardCreation,
} from "./formCreation";

interface CreateCardProps {
    children: ReactNode;
    className?: string;
}

export const CreateCard: React.FC<CreateCardProps> = ({ children, className = "" }) => {
    return (
        <div
            className={`flex min-h-[140px] w-full flex-col items-center justify-center gap-3 rounded-lg border border-dashed border-border-subtle bg-surface-subtle px-4 py-6 text-center transition-colors hover:border-primary/50 hover:bg-surface-hover dark:border-border-dark-subtle dark:bg-surface-dark-subtle dark:hover:border-primary/60 dark:hover:bg-surface-dark-hover ${className}`}
        >
            {children}
        </div>
    );
};

const AddGlyph = () => (
    <div className="flex h-8 w-8 items-center justify-center rounded-md bg-surface-muted text-text-muted dark:bg-surface-dark-muted dark:text-text-dark-muted">
        <AddIcon sx={{ fontSize: 16 }} />
    </div>
);

export const AddClusterCard = ({ namespace }: { namespace: string }) => (
    <CreateCard>
        <AddGlyph />
        <div className="flex flex-wrap items-center justify-center gap-2">
            <ClusterCreation position="card" namespace={namespace} />
            <ImportCluster position="card" namespace={namespace} />
        </div>
    </CreateCard>
);

export const AddShardCard = ({ namespace, cluster }: { namespace: string; cluster: string }) => (
    <CreateCard>
        <AddGlyph />
        <div className="flex flex-wrap items-center justify-center gap-2">
            <ShardCreation position="card" namespace={namespace} cluster={cluster} />
            <MigrateSlot position="card" namespace={namespace} cluster={cluster} />
        </div>
    </CreateCard>
);

export const AddNodeCard = ({
    namespace,
    cluster,
    shard,
}: {
    namespace: string;
    cluster: string;
    shard: string;
}) => (
    <CreateCard>
        <AddGlyph />
        <NodeCreation position="card" namespace={namespace} cluster={cluster} shard={shard} />
    </CreateCard>
);

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
}) => (
    <div className="flex h-full flex-col rounded-lg border border-border-subtle bg-surface-base p-4 dark:border-border-dark-subtle dark:bg-surface-dark-subtle">
        <div className="mb-1 text-sm font-semibold text-text-primary dark:text-text-dark-primary">
            {title}
        </div>
        {description && (
            <div className="mb-3 text-xs text-text-muted dark:text-text-dark-muted">
                {description}
            </div>
        )}
        <div className="flex-1">{children}</div>
        {tags && tags.length > 0 && (
            <div className="mt-3 flex flex-wrap gap-1">
                {tags.map((tag, i) => (
                    <Chip
                        key={i}
                        label={tag.label}
                        size="small"
                        color={(tag.color as any) || "default"}
                    />
                ))}
            </div>
        )}
    </div>
);
