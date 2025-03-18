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

import React, { ReactNode } from 'react';
import { Box, Paper, Typography, Button } from '@mui/material';

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
            className="border border-light-border dark:border-dark-border rounded-lg bg-white dark:bg-dark-paper p-10 text-center max-w-md mx-auto"
        >
            {icon && (
                <Box className="flex justify-center mb-4 text-gray-400 dark:text-gray-500">
                    {icon}
                </Box>
            )}
            <Typography variant="h6" className="font-medium mb-2 text-gray-800 dark:text-gray-100">
                {title}
            </Typography>
            <Typography variant="body2" className="text-gray-500 dark:text-gray-400 mb-6">
                {description}
            </Typography>
            {action && (
                <Button
                    variant="contained"
                    className="btn btn-primary"
                    onClick={action.onClick}
                >
                    {action.label}
                </Button>
            )}
        </Paper>
    );
};

export default EmptyState;
