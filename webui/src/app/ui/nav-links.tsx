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

import Link from "next/link";
import { usePathname } from "next/navigation";

interface NavLink {
    url: string;
    title: string;
    icon?: React.ReactNode;
    _blank?: boolean;
}

export default function NavLinks({ links }: { links: NavLink[] }) {
    const pathname = usePathname();

    return (
        <>
            {links.map((link) => {
                const isActive =
                    pathname === link.url || (link.url !== "/" && pathname.startsWith(link.url));

                return (
                    <Link
                        key={link.url}
                        href={link.url}
                        {...(link._blank ? { target: "_blank", rel: "noopener noreferrer" } : {})}
                        className={`inline-flex h-9 items-center gap-2 rounded-lg px-3 text-sm font-medium transition-colors ${
                            isActive
                                ? "bg-surface-hover text-text-primary dark:bg-surface-dark-hover dark:text-text-dark-primary"
                                : "text-text-muted hover:bg-surface-hover hover:text-text-primary dark:text-text-dark-muted dark:hover:bg-surface-dark-hover dark:hover:text-text-dark-primary"
                        }`}
                    >
                        {link.icon}
                        {link.title}
                    </Link>
                );
            })}
        </>
    );
}
