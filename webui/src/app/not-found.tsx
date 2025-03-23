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

import { Button, Typography, Box } from "@mui/material";
import Link from "next/link";
import ErrorOutlineIcon from "@mui/icons-material/ErrorOutline";
import HomeIcon from "@mui/icons-material/Home";
import ArrowBackIcon from "@mui/icons-material/ArrowBack";
import { useRouter } from "next/navigation";

export default function NotFound() {
    const router = useRouter();

    return (
        <div className="flex min-h-[calc(100vh-64px)] items-center justify-center">
            <Box className="max-w-lg p-8 text-center">
                <ErrorOutlineIcon sx={{ fontSize: 80 }} className="mb-4 text-error" />

                <Typography
                    variant="h3"
                    className="mb-2 font-bold text-gray-900 dark:text-gray-100"
                >
                    Page Not Found
                </Typography>

                <Typography variant="body1" className="mb-8 text-gray-600 dark:text-gray-300">
                    We couldn't find the page you're looking for. It might have been moved, deleted,
                    or never existed.
                </Typography>

                <div className="flex flex-wrap justify-center gap-4">
                    <Button
                        variant="contained"
                        color="primary"
                        startIcon={<HomeIcon />}
                        component={Link}
                        href="/"
                    >
                        Go to Home
                    </Button>

                    <Button
                        variant="outlined"
                        startIcon={<ArrowBackIcon />}
                        onClick={() => router.back()}
                    >
                        Go Back
                    </Button>
                </div>
            </Box>
        </div>
    );
}
