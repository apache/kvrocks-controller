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

import Image from "next/image";
import Link from "next/link";
import {
    Container,
    Typography,
    Box,
    Divider,
    Grid,
    IconButton,
    useMediaQuery,
    useTheme as useMuiTheme,
} from "@mui/material";
import {
    Launch as LaunchIcon,
    GitHub as GitHubIcon,
    Twitter as TwitterIcon,
    LinkedIn as LinkedInIcon,
    ArrowUpward as ArrowUpwardIcon,
} from "@mui/icons-material";
import { footerConfig } from "../../../config";
import { footerColumn, footerColumnItem, footerLogo } from "../lib/definitions";
import { useTheme } from "../theme-provider";

export default function Footer() {
    const { isDarkMode } = useTheme();
    const muiTheme = useMuiTheme();
    const isMobile = useMediaQuery(muiTheme.breakpoints.down("md"));

    const socialLinks = [
        { icon: <GitHubIcon />, href: "https://github.com/apache", label: "GitHub" },
        { icon: <TwitterIcon />, href: "https://x.com/TheASF", label: "Twitter" },
        {
            icon: <LinkedInIcon />,
            href: "https://www.linkedin.com/company/the-apache-software-foundation/",
            label: "LinkedIn",
        },
    ];

    const scrollToTop = () => {
        window.scrollTo({ top: 0, behavior: "smooth" });
    };

    return (
        <footer className="relative overflow-hidden">
            <div className="absolute inset-0 -z-10">
                <div className="absolute inset-0 bg-gradient-to-br from-gray-50 via-blue-50/30 to-indigo-50 dark:from-dark-paper dark:via-indigo-950/20 dark:to-gray-900"></div>

                <div className="absolute inset-0 opacity-30 dark:opacity-10">
                    <svg width="100%" height="100%" xmlns="http://www.w3.org/2000/svg">
                        <defs>
                            <pattern
                                id="footerGrid"
                                width="32"
                                height="32"
                                patternUnits="userSpaceOnUse"
                            >
                                <path
                                    d="M 32 0 L 0 0 0 32"
                                    fill="none"
                                    stroke="currentColor"
                                    strokeWidth="0.5"
                                    opacity="0.3"
                                />
                            </pattern>
                        </defs>
                        <rect width="100%" height="100%" fill="url(#footerGrid)" />
                    </svg>
                </div>

                <div className="absolute -left-32 -top-32 h-64 w-64 rounded-full bg-primary/10 blur-3xl dark:bg-primary-light/5"></div>
                <div className="absolute -bottom-32 -right-32 h-64 w-64 rounded-full bg-indigo-400/10 blur-3xl dark:bg-indigo-400/5"></div>
            </div>

            <div className="relative border-t border-gray-200/50 bg-white/60 backdrop-blur-sm dark:border-gray-700/50 dark:bg-dark-paper/60">
                <Container maxWidth="lg">
                    <div className="py-12">
                        <Grid container spacing={6}>
                            <Grid item xs={12} lg={4}>
                                <div className="space-y-6">
                                    <Link
                                        href="/"
                                        className="group inline-flex items-center space-x-3 transition-all duration-300 hover:scale-105"
                                    >
                                        <div className="relative">
                                            <Image
                                                src="/logo.svg"
                                                width={48}
                                                height={48}
                                                alt="Apache Kvrocks"
                                                className="transition-transform duration-300"
                                            />
                                            <div className="absolute inset-0 rounded-full bg-primary/20 opacity-0 blur-xl transition-opacity duration-300 group-hover:opacity-100"></div>
                                        </div>
                                        <div>
                                            <Typography
                                                variant="h6"
                                                className="font-bold text-gray-900 dark:text-gray-100"
                                            >
                                                Apache Kvrocks
                                            </Typography>
                                            <Typography
                                                variant="caption"
                                                className="text-primary dark:text-primary-light"
                                            >
                                                Controller
                                            </Typography>
                                        </div>
                                    </Link>

                                    <Typography
                                        variant="body2"
                                        className="max-w-sm leading-relaxed text-gray-600 dark:text-gray-300"
                                    >
                                        A distributed key-value NoSQL database that uses RocksDB as
                                        storage engine and is compatible with Redis protocol.
                                    </Typography>

                                    <div className="flex space-x-3">
                                        {socialLinks.map((social, index) => (
                                            <IconButton
                                                key={index}
                                                href={social.href}
                                                target="_blank"
                                                rel="noopener noreferrer"
                                                className="bg-white/50 backdrop-blur-sm transition-all duration-300 hover:-translate-y-1 hover:scale-110 hover:bg-primary/10 dark:bg-gray-800/50 dark:hover:bg-primary/20"
                                                sx={{
                                                    borderRadius: "12px",
                                                    border: "1px solid rgba(0,0,0,0.05)",
                                                    "&:hover": {
                                                        boxShadow: "0 8px 25px rgba(0,0,0,0.15)",
                                                        borderColor:
                                                            muiTheme.palette.primary.main + "40",
                                                    },
                                                }}
                                                aria-label={social.label}
                                            >
                                                {social.icon}
                                            </IconButton>
                                        ))}
                                    </div>
                                </div>
                            </Grid>

                            <Grid item xs={12} lg={8}>
                                <Grid container spacing={4}>
                                    {footerConfig.links.map((column) => (
                                        <Grid item xs={6} sm={4} key={column.title}>
                                            <FooterColumn column={column} />
                                        </Grid>
                                    ))}
                                </Grid>
                            </Grid>
                        </Grid>
                    </div>

                    <div className="border-t border-gray-200/50 py-8 dark:border-gray-700/50">
                        <div className="flex flex-col items-center justify-between space-y-6 lg:flex-row lg:space-y-0">
                            <Link
                                href={footerConfig.logo.href}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="group flex flex-col items-center space-y-2 transition-all duration-300 hover:scale-105 sm:flex-row sm:space-x-3 sm:space-y-0"
                            >
                                <Image
                                    src={footerConfig.logo.src}
                                    height={32}
                                    width={80}
                                    alt={footerConfig.logo.alt}
                                    className="transition-all duration-300 group-hover:brightness-110"
                                    style={{ width: "auto", height: "auto" }}
                                />
                                <Typography
                                    variant="caption"
                                    className="max-w-xs text-center leading-tight text-gray-500 dark:text-gray-400 sm:text-left"
                                >
                                    An Apache Software Foundation Project
                                </Typography>
                            </Link>

                            <div className="flex flex-col items-center space-y-3 sm:flex-row sm:space-x-4 sm:space-y-0">
                                <Typography
                                    variant="caption"
                                    className="max-w-lg text-center leading-relaxed text-gray-500 dark:text-gray-400 lg:text-right"
                                >
                                    {`Copyright © ${new Date().getFullYear()} ${footerConfig.copyright}`}
                                </Typography>

                                <IconButton
                                    onClick={scrollToTop}
                                    className="flex-shrink-0 bg-primary/10 transition-all duration-300 hover:scale-110 hover:bg-primary/20"
                                    sx={{
                                        borderRadius: "12px",
                                        border: "1px solid rgba(25, 118, 210, 0.2)",
                                        "&:hover": {
                                            boxShadow: "0 4px 15px rgba(25, 118, 210, 0.2)",
                                        },
                                    }}
                                    aria-label="Scroll to top"
                                >
                                    <ArrowUpwardIcon
                                        fontSize="small"
                                        className="text-primary dark:text-primary-light"
                                    />
                                </IconButton>
                            </div>
                        </div>
                    </div>
                </Container>
            </div>
        </footer>
    );
}

const FooterColumn = ({ column }: { column: footerColumn }) => (
    <div className="space-y-4">
        <Typography
            variant="subtitle1"
            className="relative pb-2 font-bold text-gray-900 dark:text-gray-100"
        >
            {column.title}
            <div className="absolute bottom-0 left-0 h-0.5 w-8 rounded-full bg-gradient-to-r from-primary to-primary-light"></div>
        </Typography>
        <ul className="space-y-3">
            {column.items.map((item) => (
                <FooterLink key={item.label} item={item} />
            ))}
        </ul>
    </div>
);

const FooterLink = ({ item }: { item: footerColumnItem }) => (
    <li>
        <Link
            href={item.href || item.to || "/"}
            target={item.href ? "_blank" : undefined}
            rel={item.href ? "noopener noreferrer" : undefined}
            className="group inline-flex items-center text-gray-600 transition-all duration-300 hover:text-primary dark:text-gray-300 dark:hover:text-primary-light"
        >
            <span className="relative">
                {item.label}
                <span className="absolute -bottom-0.5 left-0 h-0.5 w-0 rounded-full bg-gradient-to-r from-primary to-primary-light transition-all duration-300 group-hover:w-full"></span>
            </span>
            {item.href && (
                <LaunchIcon
                    className="ml-1.5 text-gray-400 transition-all duration-300 group-hover:-translate-y-0.5 group-hover:translate-x-0.5 group-hover:transform group-hover:text-primary dark:text-gray-500 dark:group-hover:text-primary-light"
                    sx={{ fontSize: 14 }}
                />
            )}
        </Link>
    </li>
);
