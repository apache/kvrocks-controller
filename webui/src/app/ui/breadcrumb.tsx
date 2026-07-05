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

import { usePathname } from "next/navigation";
import Link from "next/link";
import { useMemo } from "react";
import ChevronRightIcon from "@mui/icons-material/ChevronRight";

interface Crumb {
    label: string;
    href: string | null;
}

export default function Breadcrumb() {
    const pathname = usePathname();

    const crumbs = useMemo<Crumb[]>(() => {
        if (pathname === "/") return [];
        const parts = pathname.split("/").filter(Boolean);
        const out: Crumb[] = [];

        parts.forEach((segment, index) => {
            const href = "/" + parts.slice(0, index + 1).join("/");
            const isLast = index === parts.length - 1;
            const prev = parts[index - 1];
            const numeric = !isNaN(Number(segment));

            let label = segment;
            if (index === 0 && segment === "namespaces") label = "Namespaces";
            else if (segment === "clusters") label = "Clusters";
            else if (segment === "shards") label = "Shards";
            else if (segment === "nodes") label = "Nodes";
            else if (numeric && prev === "shards") label = `Shard ${Number(segment) + 1}`;
            else if (numeric && prev === "nodes") label = `Node ${Number(segment) + 1}`;
            else if (numeric) label = segment;

            // Container-only segments have no destination page.
            const containerOnly =
                (segment === "clusters" || segment === "shards" || segment === "nodes") && !isLast;

            out.push({ label, href: isLast || containerOnly ? null : href });
        });

        return out;
    }, [pathname]);

    if (crumbs.length === 0) return null;

    return (
        <nav
            aria-label="Breadcrumb"
            className="border-b border-border-subtle bg-surface-base dark:border-border-dark-subtle dark:bg-surface-dark-base"
        >
            <div className="mx-auto flex max-w-[1440px] items-center gap-1.5 px-6 py-3 text-sm">
                <Link
                    href="/"
                    className="text-text-muted transition-colors hover:text-text-primary dark:text-text-dark-muted dark:hover:text-text-dark-primary"
                >
                    Home
                </Link>
                {crumbs.map((crumb, i) => (
                    <span key={i} className="flex items-center gap-1.5">
                        <ChevronRightIcon
                            sx={{ fontSize: 14 }}
                            className="text-text-muted/60 dark:text-text-dark-muted/60"
                        />
                        {crumb.href ? (
                            <Link
                                href={crumb.href}
                                className="text-text-muted transition-colors hover:text-text-primary dark:text-text-dark-muted dark:hover:text-text-dark-primary"
                            >
                                {crumb.label}
                            </Link>
                        ) : (
                            <span className="font-medium text-text-primary dark:text-text-dark-primary">
                                {crumb.label}
                            </span>
                        )}
                    </span>
                ))}
            </div>
        </nav>
    );
}
