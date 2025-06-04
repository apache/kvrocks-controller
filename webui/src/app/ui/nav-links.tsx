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
    scrolled = false,
}: {
    links: Array<{
        url: string;
        title: string;
        icon?: React.ReactNode;
        _blank?: boolean;
    }>;
    scrolled?: boolean;
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
                            sx={{
                                textTransform: "none",
                                borderRadius: "20px",
                                paddingLeft: scrolled ? 2 : 2.5,
                                paddingRight: scrolled ? 2 : 2.5,
                                paddingTop: scrolled ? 0.6 : 0.8,
                                paddingBottom: scrolled ? 0.6 : 0.8,
                                fontSize: scrolled ? "0.875rem" : "0.9rem",
                                letterSpacing: "0.01em",
                                fontWeight: 500,
                                marginX: 0.5,
                                transition: "all 0.3s ease",
                                backgroundColor: isActive
                                    ? (theme) =>
                                          theme.palette.mode === "dark"
                                              ? "rgba(255, 255, 255, 0.15)"
                                              : "rgba(25, 118, 210, 0.08)"
                                    : "transparent",
                                color: (theme) => {
                                    if (theme.palette.mode === "dark") {
                                        return isActive ? "#fff" : "rgba(255, 255, 255, 0.9)";
                                    }
                                    return isActive ? "#1976d2" : "rgba(0, 0, 0, 0.7)";
                                },
                                "&:hover": {
                                    backgroundColor: (theme) =>
                                        theme.palette.mode === "dark"
                                            ? "rgba(255, 255, 255, 0.2)"
                                            : "rgba(25, 118, 210, 0.12)",
                                    boxShadow: isActive
                                        ? (theme) =>
                                              theme.palette.mode === "dark"
                                                  ? "0 2px 8px rgba(0, 0, 0, 0.3)"
                                                  : "0 2px 8px rgba(25, 118, 210, 0.2)"
                                        : "none",
                                },
                                boxShadow: isActive
                                    ? (theme) =>
                                          theme.palette.mode === "dark"
                                              ? "0 2px 5px rgba(0, 0, 0, 0.2)"
                                              : "0 2px 5px rgba(25, 118, 210, 0.15)"
                                    : "none",
                            }}
                            startIcon={link.icon}
                            size="small"
                        >
                            {link.title}
                        </Button>
                    </Link>
                );
            })}
        </>
    );
}
