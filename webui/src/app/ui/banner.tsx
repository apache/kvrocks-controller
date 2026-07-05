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

import { IconButton, Tooltip } from "@mui/material";
import Image from "next/image";
import NavLinks from "./nav-links";
import { useTheme } from "../theme-provider";
import Brightness4Icon from "@mui/icons-material/Brightness4";
import Brightness7Icon from "@mui/icons-material/Brightness7";
import GitHubIcon from "@mui/icons-material/GitHub";
import HomeIcon from "@mui/icons-material/Home";
import FolderIcon from "@mui/icons-material/Folder";
import MenuBookIcon from "@mui/icons-material/MenuBook";
import SearchIcon from "@mui/icons-material/Search";
import Link from "next/link";

const links = [
    { url: "/", title: "Home", icon: <HomeIcon sx={{ fontSize: 16 }} /> },
    { url: "/namespaces", title: "Namespaces", icon: <FolderIcon sx={{ fontSize: 16 }} /> },
    {
        url: "https://kvrocks.apache.org",
        title: "Docs",
        icon: <MenuBookIcon sx={{ fontSize: 16 }} />,
        _blank: true,
    },
];

export default function Banner() {
    const { isDarkMode, toggleTheme } = useTheme();

    const openSearch = () => {
        window.dispatchEvent(
            new KeyboardEvent("keydown", { key: "k", metaKey: true, ctrlKey: true })
        );
    };

    return (
        <header
            id="navbar"
            className="fixed inset-x-0 top-0 z-40 h-[var(--lin-topbar-height)] border-b border-border-subtle bg-surface-base/95 backdrop-blur-md dark:border-border-dark-subtle dark:bg-surface-dark-base/85"
        >
            <div className="mx-auto flex h-full max-w-[1440px] items-center gap-5 px-6">
                <Link href="/" className="flex items-center gap-2.5 no-underline">
                    <Image
                        src="/logo.svg"
                        width={24}
                        height={24}
                        alt="Apache Kvrocks"
                        className="shrink-0"
                        style={{ height: "auto" }}
                    />
                    <span className="hidden text-base font-semibold tracking-tight text-text-primary dark:text-text-dark-primary sm:block">
                        Kvrocks
                    </span>
                    <span className="hidden text-2xs font-medium uppercase tracking-wider text-text-muted dark:text-text-dark-muted sm:block">
                        Controller
                    </span>
                </Link>

                <nav className="ml-3 hidden items-center gap-1 md:flex">
                    <NavLinks links={links} />
                </nav>

                <div className="flex-1" />

                <button
                    onClick={openSearch}
                    className="hidden h-9 items-center gap-2.5 rounded-lg border border-border-subtle bg-surface-subtle px-3 text-sm text-text-muted transition-colors hover:border-border-strong hover:text-text-secondary dark:border-border-dark-subtle dark:bg-surface-dark-subtle dark:text-text-dark-muted dark:hover:border-border-dark-strong dark:hover:text-text-dark-secondary sm:flex"
                >
                    <SearchIcon sx={{ fontSize: 16 }} />
                    <span>Search…</span>
                    <span className="ml-8 flex items-center gap-0.5">
                        <kbd>⌘</kbd>
                        <kbd>K</kbd>
                    </span>
                </button>

                <button
                    onClick={openSearch}
                    aria-label="Search"
                    className="flex h-9 w-9 items-center justify-center rounded-lg text-text-muted transition-colors hover:bg-surface-hover hover:text-text-primary dark:text-text-dark-muted dark:hover:bg-surface-dark-hover dark:hover:text-text-dark-primary sm:hidden"
                >
                    <SearchIcon sx={{ fontSize: 16 }} />
                </button>

                <Tooltip title={isDarkMode ? "Light theme" : "Dark theme"} arrow>
                    <IconButton
                        onClick={toggleTheme}
                        size="small"
                        sx={{ width: 34, height: 34 }}
                        aria-label="Toggle theme"
                    >
                        {isDarkMode ? (
                            <Brightness7Icon sx={{ fontSize: 18 }} />
                        ) : (
                            <Brightness4Icon sx={{ fontSize: 18 }} />
                        )}
                    </IconButton>
                </Tooltip>

                <Tooltip title="GitHub" arrow>
                    <IconButton
                        href="https://github.com/apache/kvrocks-controller"
                        target="_blank"
                        rel="noopener noreferrer"
                        size="small"
                        sx={{ width: 34, height: 34 }}
                        aria-label="GitHub"
                    >
                        <GitHubIcon sx={{ fontSize: 18 }} />
                    </IconButton>
                </Tooltip>
            </div>
        </header>
    );
}
