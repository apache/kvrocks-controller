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

import React from 'react';
import { Box, CircularProgress, Typography, Fade } from '@mui/material';

interface LoadingSpinnerProps {
    message?: string;
    size?: 'small' | 'medium' | 'large';
    fullScreen?: boolean;
}

export const LoadingSpinner: React.FC<LoadingSpinnerProps> = ({ 
    message = 'Loading...', 
    size = 'medium',
    fullScreen = false
}) => {
    const spinnerSize = {
        small: 24,
        medium: 40,
        large: 60
    }[size];

    return (
        <Fade in={true} timeout={300}>
            <Box
                sx={{
                    display: 'flex',
                    flexDirection: 'column',
                    alignItems: 'center',
                    justifyContent: 'center',
                    height: fullScreen ? '100vh' : '100%',
                    width: '100%',
                    minHeight: fullScreen ? '100vh' : '300px',
                }}
                className="text-primary dark:text-primary-light"
            >
                <CircularProgress size={spinnerSize} thickness={4} className="text-primary dark:text-primary-light" />
                {message && (
                    <Typography 
                        variant="body2" 
                        className="mt-4 text-gray-600 dark:text-gray-300 animate-pulse"
                    >
                        {message}
                    </Typography>
                )}
            </Box>
        </Fade>
    );
};