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
import { Box, Paper, Typography, Button } from "@mui/material";

interface EmptyStateProps {
    title: string;
    description: string;
    icon?: ReactNode;
    action?: {
        label: string;
        onClick: () => void;
    };
}

const EmptyState: React.FC<EmptyStateProps> = ({ title, description, icon, action }) => {
    return (
        <Paper
            elevation={0}
            className="mx-auto max-w-md rounded-2xl border border-gray-100 bg-white p-12 text-center shadow-sm dark:border-gray-800 dark:bg-dark-paper"
        >
            {icon && <Box className="mb-6 flex justify-center">{icon}</Box>}
            <Typography variant="h5" className="mb-3 font-medium text-gray-800 dark:text-gray-100">
                {title}
            </Typography>
            <Typography variant="body1" className="mb-8 text-gray-500 dark:text-gray-400">
                {description}
            </Typography>
            {action && (
                <Button
                    variant="contained"
                    color="primary"
                    className="rounded-full px-6 py-2.5 font-medium shadow-md transition-all hover:shadow-lg"
                    onClick={action.onClick}
                    disableElevation
                >
                    {action.label}
                </Button>
            )}
        </Paper>
    );
};

export default EmptyState;
