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

import { AppBar, Container, Toolbar, IconButton, Box, Tooltip, Typography } from "@mui/material";
import Image from "next/image";
import NavLinks from "./nav-links";
import { useTheme } from "../theme-provider";
import Brightness4Icon from "@mui/icons-material/Brightness4";
import Brightness7Icon from "@mui/icons-material/Brightness7";
import GitHubIcon from "@mui/icons-material/GitHub";
import HomeIcon from "@mui/icons-material/Home";
import FolderIcon from "@mui/icons-material/Folder";
import MenuBookIcon from "@mui/icons-material/MenuBook";
import { usePathname } from "next/navigation";
import { useEffect, useState } from "react";

const links = [
    {
        url: "/",
        title: "Home",
        icon: <HomeIcon fontSize="small" />,
    },
    {
        url: "/namespaces",
        title: "Namespaces",
        icon: <FolderIcon fontSize="small" />,
    },
    {
        url: "https://kvrocks.apache.org",
        title: "Documentation",
        icon: <MenuBookIcon fontSize="small" />,
        _blank: true,
    },
];

export default function Banner() {
    const { isDarkMode, toggleTheme } = useTheme();
    const pathname = usePathname();
    const [mounted, setMounted] = useState(false);

    useEffect(() => {
        const storedTheme = localStorage.getItem("theme");
        const prefersDark = window.matchMedia("(prefers-color-scheme: dark)").matches;
        const shouldBeDark = storedTheme === "dark" || (!storedTheme && prefersDark);

        if (shouldBeDark) {
            document.getElementById("navbar")?.classList.add("navbar-dark-mode");
        }

        setMounted(true);
    }, []);

    // Generate breadcrumb from pathname
    const breadcrumbs = pathname.split("/").filter(Boolean);

    return (
        <AppBar
            position="fixed"
            elevation={0}
            id="navbar"
            className={`backdrop-blur-sm transition-colors duration-300 ${
                isDarkMode
                    ? "navbar-dark-mode bg-opacity-95"
                    : "bg-white bg-opacity-95 text-gray-800"
            }`}
            sx={{
                bgcolor: isDarkMode
                    ? "rgba(21, 101, 192, 0.98) !important"
                    : "rgba(255, 255, 255, 0.98)",
                backdropFilter: "blur(8px)",
                borderBottom: isDarkMode
                    ? "1px solid rgba(30, 64, 175, 0.3)"
                    : "1px solid rgba(229, 231, 235, 0.6)",
            }}
        >
            <Container maxWidth={false}>
                <Toolbar className="flex justify-between">
                    <div className="flex items-center">
                        <Image src="/logo.svg" width={40} height={40} alt="logo" className="mr-4" />
                        <Typography
                            variant="h6"
                            component="div"
                            className="hidden font-medium text-primary dark:text-primary-light sm:block"
                        >
                            Apache Kvrocks Controller
                        </Typography>
                    </div>

                    <Box className="hidden items-center space-x-1 md:flex">
                        <NavLinks links={links} />
                    </Box>

                    <Box className="flex items-center">
                        {breadcrumbs.length > 0 && (
                            <Box className="mr-4 hidden items-center rounded-md bg-gray-100 px-4 py-1 text-sm dark:bg-dark-border md:flex">
                                {breadcrumbs.map((breadcrumb, i) => (
                                    <Typography
                                        key={i}
                                        variant="body2"
                                        className="text-gray-500 dark:text-gray-400"
                                    >
                                        {i > 0 && " / "}
                                        {breadcrumb}
                                    </Typography>
                                ))}
                            </Box>
                        )}

                        <Tooltip title="Toggle dark mode">
                            <IconButton onClick={toggleTheme} color="inherit" size="small">
                                {isDarkMode ? <Brightness7Icon /> : <Brightness4Icon />}
                            </IconButton>
                        </Tooltip>

                        <Tooltip title="GitHub Repository">
                            <IconButton
                                color="inherit"
                                href="https://github.com/apache/kvrocks-controller"
                                target="_blank"
                                size="small"
                                className="ml-2"
                            >
                                <GitHubIcon />
                            </IconButton>
                        </Tooltip>
                    </Box>
                </Toolbar>
            </Container>
        </AppBar>
    );
}
