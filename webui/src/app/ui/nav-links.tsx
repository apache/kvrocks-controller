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
import { usePathname } from "next/navigation";

export default function NavLinks({
    links,
}: {
    links: Array<{
        url: string;
        title: string;
        icon?: React.ReactNode;
        _blank?: boolean;
    }>;
}) {
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
                        passHref
                        {...(link._blank ? { target: "_blank", rel: "noopener noreferrer" } : {})}
                    >
                        <Button
                            color="inherit"
                            className={`mx-1 rounded-full px-4 py-1 transition-colors ${
                                isActive
                                    ? "bg-primary-light/10 text-primary dark:text-primary-light"
                                    : "hover:bg-gray-100 dark:hover:bg-dark-border"
                            }`}
                            startIcon={link.icon}
                        >
                            {link.title}
                        </Button>
                    </Link>
                );
            })}
        </>
    );
}
