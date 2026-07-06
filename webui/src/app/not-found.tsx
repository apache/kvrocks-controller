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

import { Button } from "@mui/material";
import Link from "next/link";
import HomeIcon from "@mui/icons-material/Home";
import ArrowBackIcon from "@mui/icons-material/ArrowBack";
import { useRouter } from "next/navigation";

export default function NotFound() {
    const router = useRouter();

    return (
        <div className="flex min-h-[calc(100vh-var(--lin-topbar-height))] items-center justify-center px-6 py-16">
            <div className="w-full max-w-md rounded-lg border border-border-subtle bg-surface-subtle p-8 text-center dark:border-border-dark-subtle dark:bg-surface-dark-subtle">
                <div className="lin-eyebrow mb-2">404</div>
                <h1 className="mb-1 text-xl font-semibold text-text-primary dark:text-text-dark-primary">
                    Page not found
                </h1>
                <p className="mb-6 text-xs text-text-muted dark:text-text-dark-muted">
                    The page you&apos;re looking for doesn&apos;t exist or has been moved.
                </p>
                <div className="flex flex-wrap justify-center gap-2">
                    <Button
                        variant="contained"
                        size="small"
                        component={Link}
                        href="/"
                        startIcon={<HomeIcon sx={{ fontSize: 13 }} />}
                    >
                        Home
                    </Button>
                    <Button
                        variant="outlined"
                        size="small"
                        onClick={() => router.back()}
                        startIcon={<ArrowBackIcon sx={{ fontSize: 13 }} />}
                    >
                        Go back
                    </Button>
                </div>
            </div>
        </div>
    );
}
