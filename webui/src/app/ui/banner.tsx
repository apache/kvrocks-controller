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
import Link from "next/link";

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
    const [scrolled, setScrolled] = useState(false);

    useEffect(() => {
        const storedTheme = localStorage.getItem("theme");
        const prefersDark = window.matchMedia("(prefers-color-scheme: dark)").matches;
        const shouldBeDark = storedTheme === "dark" || (!storedTheme && prefersDark);

        if (shouldBeDark) {
            document.getElementById("navbar")?.classList.add("navbar-dark-mode");
        }

        const handleScroll = () => {
            if (window.scrollY > 10) {
                setScrolled(true);
            } else {
                setScrolled(false);
            }
        };

        window.addEventListener("scroll", handleScroll);
        setMounted(true);

        return () => {
            window.removeEventListener("scroll", handleScroll);
        };
    }, []);

    return (
        <AppBar
            position="fixed"
            elevation={0}
            id="navbar"
            className="transition-all duration-300"
            sx={{
                backdropFilter: "blur(10px)",
                backgroundColor: isDarkMode
                    ? scrolled
                        ? "rgba(21, 101, 192, 0.95)"
                        : "rgba(21, 101, 192, 0.85)"
                    : scrolled
                      ? "rgba(255, 255, 255, 0.95)"
                      : "rgba(255, 255, 255, 0.85)",
                boxShadow: scrolled
                    ? isDarkMode
                        ? "0 4px 20px rgba(0,0,0,0.2)"
                        : "0 4px 20px rgba(0,0,0,0.1)"
                    : "none",
                borderBottom: scrolled
                    ? "none"
                    : isDarkMode
                      ? "1px solid rgba(255,255,255,0.1)"
                      : "1px solid rgba(0,0,0,0.05)",
                height: scrolled ? "60px" : "72px",
            }}
        >
            <Container maxWidth={false} sx={{ px: { xs: 2, md: 4 } }}>
                <Toolbar
                    disableGutters
                    sx={{
                        height: scrolled ? "60px" : "72px",
                        minHeight: "unset !important",
                        transition: "height 0.3s ease",
                    }}
                >
                    <Link
                        href="/"
                        className="flex items-center no-underline transition-all duration-300"
                    >
                        <Box className="relative flex items-center transition-all duration-300">
                            <Image
                                src="/logo.svg"
                                width={scrolled ? 36 : 40}
                                height={scrolled ? 36 : 40}
                                alt="Apache Kvrocks"
                                className={`mr-3 transition-all duration-300 ${
                                    isDarkMode ? "brightness-110 filter" : ""
                                }`}
                                style={{ height: "auto" }} // Add style to maintain aspect ratio
                            />
                            <Box className="flex flex-col">
                                <Typography
                                    variant={scrolled ? "subtitle1" : "h6"}
                                    component="div"
                                    sx={{
                                        lineHeight: 1.2,
                                        letterSpacing: "0.01em",
                                        fontSize: scrolled ? "1rem" : "1.125rem",
                                        fontWeight: 500,
                                        transition: "all 0.3s ease",
                                        color: isDarkMode ? "#fff" : "#1976d2",
                                    }}
                                    className="hidden sm:block"
                                >
                                    Apache Kvrocks
                                </Typography>
                                <Typography
                                    variant="caption"
                                    sx={{
                                        fontSize: scrolled ? "0.7rem" : "0.75rem",
                                        opacity: scrolled ? 0.9 : 1,
                                        fontWeight: 500,
                                        transition: "all 0.3s ease",
                                        color: isDarkMode ? "rgba(255,255,255,0.8)" : "#42a5f5",
                                    }}
                                    className="hidden sm:block"
                                >
                                    Controller
                                </Typography>
                            </Box>
                        </Box>
                    </Link>

                    <Box sx={{ flexGrow: 1 }} />

                    <Box className="hidden items-center gap-1 md:flex">
                        <NavLinks links={links} scrolled={scrolled} />
                    </Box>

                    <Box className="flex items-center">
                        <Tooltip title="Toggle dark mode">
                            <IconButton
                                onClick={toggleTheme}
                                size="small"
                                sx={{
                                    width: scrolled ? 32 : 36,
                                    height: scrolled ? 32 : 36,
                                    padding: 0.75,
                                    backgroundColor: isDarkMode
                                        ? "rgba(255,255,255,0.1)"
                                        : "rgba(0,0,0,0.05)",
                                    transition: "all 0.3s ease",
                                    "&:hover": {
                                        backgroundColor: isDarkMode
                                            ? "rgba(255,255,255,0.2)"
                                            : "rgba(0,0,0,0.08)",
                                    },
                                    marginLeft: 1,
                                    borderRadius: "50%",
                                }}
                            >
                                {isDarkMode ? (
                                    <Brightness7Icon
                                        fontSize="small"
                                        sx={{ color: "rgba(255,255,255,0.9)" }}
                                    />
                                ) : (
                                    <Brightness4Icon
                                        fontSize="small"
                                        sx={{ color: "rgba(0,0,0,0.7)" }}
                                    />
                                )}
                            </IconButton>
                        </Tooltip>

                        <Tooltip title="GitHub Repository">
                            <IconButton
                                href="https://github.com/apache/kvrocks-controller"
                                target="_blank"
                                size="small"
                                sx={{
                                    width: scrolled ? 32 : 36,
                                    height: scrolled ? 32 : 36,
                                    padding: 0.75,
                                    backgroundColor: isDarkMode
                                        ? "rgba(255,255,255,0.1)"
                                        : "rgba(0,0,0,0.05)",
                                    transition: "all 0.3s ease",
                                    "&:hover": {
                                        backgroundColor: isDarkMode
                                            ? "rgba(255,255,255,0.2)"
                                            : "rgba(0,0,0,0.08)",
                                    },
                                    marginLeft: 1,
                                    borderRadius: "50%",
                                }}
                            >
                                <GitHubIcon
                                    fontSize="small"
                                    sx={{
                                        color: isDarkMode
                                            ? "rgba(255,255,255,0.9)"
                                            : "rgba(0,0,0,0.7)",
                                    }}
                                />
                            </IconButton>
                        </Tooltip>
                    </Box>
                </Toolbar>
            </Container>
        </AppBar>
    );
}
