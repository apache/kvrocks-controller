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

import React from "react";
import { CircularProgress } from "@mui/material";

interface LoadingSpinnerProps {
    message?: string;
    size?: "small" | "medium" | "large";
    fullScreen?: boolean;
}

export const LoadingSpinner: React.FC<LoadingSpinnerProps> = ({
    message = "Loading…",
    size = "medium",
    fullScreen = false,
}) => {
    const px = { small: 14, medium: 18, large: 24 }[size];

    return (
        <div
            className="flex w-full flex-col items-center justify-center gap-2 text-text-muted dark:text-text-dark-muted"
            style={{ minHeight: fullScreen ? "100vh" : "240px" }}
        >
            <CircularProgress size={px} thickness={5} sx={{ color: "primary.main" }} />
            {message && <span className="text-xs">{message}</span>}
        </div>
    );
};
