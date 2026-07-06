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

import React, { ReactNode } from "react";
import { Button } from "@mui/material";

interface EmptyStateProps {
    title: string;
    description: string;
    icon?: ReactNode;
    action?: {
        label: string;
        onClick: () => void;
    };
}

const EmptyState: React.FC<EmptyStateProps> = ({ title, description, icon, action }) => (
    <div className="mx-auto flex max-w-md flex-col items-center rounded-xl border border-dashed border-border-subtle bg-surface-subtle px-10 py-14 text-center dark:border-border-dark-subtle dark:bg-surface-dark-subtle">
        {icon && (
            <div className="mb-5 flex h-12 w-12 items-center justify-center rounded-xl bg-surface-muted text-text-muted dark:bg-surface-dark-muted dark:text-text-dark-muted">
                {icon}
            </div>
        )}
        <div className="mb-1.5 text-base font-semibold text-text-primary dark:text-text-dark-primary">
            {title}
        </div>
        <p className="mb-7 text-sm text-text-muted dark:text-text-dark-muted">{description}</p>
        {action && (
            <Button variant="contained" onClick={action.onClick}>
                {action.label}
            </Button>
        )}
    </div>
);

export default EmptyState;
