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

import React, { useCallback, useMemo } from "react";
import mainImage from "./main.png";
import {
    Button,
    Typography,
    Box,
    Grid,
    Chip,
    Card,
    CardContent,
    Container,
    useMediaQuery,
    useTheme as useMuiTheme,
} from "@mui/material";
import Image from "next/image";
import { useRouter } from "next/navigation";
import { useTheme } from "./theme-provider";
import StorageIcon from "@mui/icons-material/Storage";
import SyncIcon from "@mui/icons-material/Sync";
import SecurityIcon from "@mui/icons-material/Security";
import HubIcon from "@mui/icons-material/Hub";
import GitHubIcon from "@mui/icons-material/GitHub";
import LaunchIcon from "@mui/icons-material/Launch";
import MenuBookIcon from "@mui/icons-material/MenuBook";
import ArrowForwardIcon from "@mui/icons-material/ArrowForward";
import CheckCircleOutlineIcon from "@mui/icons-material/CheckCircleOutline";
import SpeedIcon from "@mui/icons-material/Speed";
import CloudIcon from "@mui/icons-material/Cloud";
import TerminalIcon from "@mui/icons-material/Terminal";
import Link from "next/link";
import { useEffect, useState, useRef } from "react";

export default function Home() {
    const router = useRouter();
    const { isDarkMode } = useTheme();
    const muiTheme = useMuiTheme();
    const isMobile = useMediaQuery(muiTheme.breakpoints.down("md"));
    const [isLoaded, setIsLoaded] = useState(true);
    const [scrollY, setScrollY] = useState(0);
    const [cursorPosition, setCursorPosition] = useState({ x: 0, y: 0 });
    const [cursorVisible, setCursorVisible] = useState(true);
    const requestRef = useRef<number>();
    const prevScrollY = useRef(0);

    const terminalRef = useRef({ lineIndex: 0, charIndex: 0 });

    const terminalLines = useMemo(
        () => [
            { text: "$ redis-cli -p 6666", className: "text-green-400" },
            { text: '127.0.0.1:6666> SET mykey "Hello Kvrocks"', className: "text-blue-400" },
            { text: "OK", className: "text-yellow-400" },
            { text: "127.0.0.1:6666> GET mykey", className: "text-blue-400" },
            { text: '"Hello Kvrocks"', className: "text-yellow-400" },
            { text: "127.0.0.1:6666> INFO", className: "text-blue-400" },
            { text: "# Server", className: "text-yellow-200" },
            { text: "kvrocks_version:unstable", className: "text-yellow-400" },
            { text: "kvrocks_git_sha1:fffffff", className: "text-yellow-400" },
        ],
        []
    );

    const handleScroll = useCallback(() => {
        if (Math.abs(window.scrollY - prevScrollY.current) > 5) {
            setScrollY(window.scrollY);
            prevScrollY.current = window.scrollY;
        }
        requestRef.current = requestAnimationFrame(handleScroll);
    }, []);

    useEffect(() => {
        requestRef.current = requestAnimationFrame(handleScroll);

        const cursorInterval = setInterval(() => {
            setCursorVisible((prev) => !prev);
        }, 800);

        const typeNextChar = () => {
            const { lineIndex, charIndex } = terminalRef.current;

            if (lineIndex < terminalLines.length) {
                const line = terminalLines[lineIndex];

                if (charIndex <= line.text.length) {
                    setCursorPosition({
                        x: charIndex,
                        y: lineIndex,
                    });
                    terminalRef.current.charIndex++;
                } else {
                    terminalRef.current.lineIndex++;
                    terminalRef.current.charIndex = 0;
                }
            }
        };

        const typeInterval = setInterval(typeNextChar, 50);

        return () => {
            if (requestRef.current) {
                cancelAnimationFrame(requestRef.current);
            }
            clearInterval(cursorInterval);
            clearInterval(typeInterval);
        };
    }, [handleScroll, terminalLines]);

    const features = useMemo(
        () => [
            {
                title: "Namespace",
                description: "Similar to Redis SELECT but equipped with token per namespace",
                icon: <StorageIcon sx={{ fontSize: 40 }} />,
                color: isDarkMode ? "#42a5f5" : "#1976d2",
                delay: 100,
            },
            {
                title: "Replication",
                description: "Async replication using binlog like MySQL",
                icon: <SyncIcon sx={{ fontSize: 40 }} />,
                color: isDarkMode ? "#ba68c8" : "#9c27b0",
                delay: 200,
            },
            {
                title: "High Availability",
                description: "Support Redis sentinel to failover when master or replica was failed",
                icon: <SecurityIcon sx={{ fontSize: 40 }} />,
                color: isDarkMode ? "#4caf50" : "#2e7d32",
                delay: 300,
            },
            {
                title: "Cluster",
                description: "Centralized management but accessible via any Redis cluster client",
                icon: <HubIcon sx={{ fontSize: 40 }} />,
                color: isDarkMode ? "#ff9800" : "#ed6c02",
                delay: 400,
            },
        ],
        [isDarkMode]
    );

    const resources = useMemo(
        () => [
            {
                title: "Documentation",
                description: "Learn how to use Kvrocks Controller",
                icon: <MenuBookIcon />,
                url: "https://kvrocks.apache.org/docs/",
                color: "#1976d2",
            },
            {
                title: "GitHub Repository",
                description: "View the source code on GitHub",
                icon: <GitHubIcon />,
                url: "https://github.com/apache/kvrocks-controller",
                color: "#333333",
            },
        ],
        []
    );

    const benefits = useMemo(
        () => [
            {
                title: "Enterprise-grade reliability",
                description: "Built to handle mission-critical workloads with fault tolerance",
                icon: <SecurityIcon sx={{ fontSize: 28 }} />,
            },
            {
                title: "High performance data access",
                description: "Optimized for speed with RocksDB as the storage engine",
                icon: <SpeedIcon sx={{ fontSize: 28 }} />,
            },
            {
                title: "Simplified cluster management",
                description: "Intuitive UI for managing your distributed database infrastructure",
                icon: <HubIcon sx={{ fontSize: 28 }} />,
            },
            {
                title: "Seamless Redis compatibility",
                description: "Works with existing Redis clients and tools",
                icon: <CloudIcon sx={{ fontSize: 28 }} />,
            },
        ],
        []
    );

    const heroParallax = {
        transform: `translate3d(0, ${scrollY * 0.3}px, 0)`,
        willChange: "transform",
        transition: "transform 0.05s linear",
    };

    const featureSectionStyle = {
        opacity: 1,
        transform: `translate3d(0, ${Math.min(0, (scrollY - 300) * 0.05)}px, 0)`,
        willChange: "transform",
    };

    const handleGetStarted = useCallback(() => router.push("/namespaces"), [router]);

    return (
        <div className="flex min-h-screen flex-col overflow-x-hidden will-change-scroll">
            {/* Hero Section */}
            <section className="relative flex min-h-[90vh] items-center justify-center overflow-hidden px-4 py-16 md:py-0">
                <div className="absolute inset-0 -z-10">
                    <div className="absolute inset-0 bg-gradient-to-br from-blue-50/80 via-white to-indigo-50/80 dark:from-dark/80 dark:via-dark dark:to-indigo-900/10"></div>

                    <div className="absolute left-0 top-0 h-full w-full">
                        <div className="h-full w-full opacity-10 dark:opacity-5">
                            <svg
                                width="100%"
                                height="100%"
                                xmlns="http://www.w3.org/2000/svg"
                                style={{ position: "absolute" }}
                            >
                                <defs>
                                    <pattern
                                        id="grid"
                                        width="40"
                                        height="40"
                                        patternUnits="userSpaceOnUse"
                                    >
                                        <path
                                            d="M 40 0 L 0 0 0 40"
                                            fill="none"
                                            stroke="currentColor"
                                            strokeWidth="0.5"
                                        />
                                    </pattern>
                                </defs>
                                <rect width="100%" height="100%" fill="url(#grid)" />
                            </svg>
                        </div>
                    </div>

                    <div className="absolute -right-48 -top-48 h-96 w-96 rounded-full bg-primary/20 blur-3xl will-change-transform dark:bg-primary-light/10"></div>
                    <div className="absolute -bottom-48 -left-48 h-96 w-96 rounded-full bg-indigo-400/20 blur-3xl will-change-transform dark:bg-indigo-400/10"></div>
                </div>

                <Container maxWidth="lg" className="relative z-10">
                    <Grid container spacing={4} alignItems="center" justifyContent="center">
                        <Grid
                            item
                            xs={12}
                            md={6}
                            className={`${isMobile ? "order-2" : "order-1"} transition-all duration-700 ${isLoaded ? "translate-y-0 opacity-100" : "translate-y-8 opacity-0"}`}
                            style={heroParallax}
                        >
                            <Box className="space-y-6 md:pr-8">
                                <div className="inline-flex items-center rounded-full bg-gradient-to-r from-primary/80 to-primary px-4 py-1.5 text-white shadow-lg">
                                    <span className="mr-2 text-xs font-medium uppercase tracking-wider">
                                        Apache Software Foundation
                                    </span>
                                    <span className="flex h-2 w-2 animate-pulse rounded-full bg-white"></span>
                                </div>

                                <Typography
                                    variant="h1"
                                    component="h1"
                                    className="mt-6 font-bold leading-tight text-gray-900 dark:text-gray-100"
                                    sx={{
                                        fontSize: { xs: "2.5rem", sm: "3rem", md: "3.5rem" },
                                        background: "linear-gradient(45deg, #1976d2, #42a5f5)",
                                        WebkitBackgroundClip: "text",
                                        WebkitTextFillColor: "transparent",
                                        textShadow: isDarkMode
                                            ? "0 5px 10px rgba(0,0,0,0.5)"
                                            : "none",
                                    }}
                                >
                                    Apache Kvrocks™ <span className="block">Controller</span>
                                </Typography>

                                <Typography
                                    variant="h6"
                                    className="max-w-xl font-light text-gray-600 dark:text-gray-300"
                                    sx={{ fontSize: { xs: "1rem", sm: "1.1rem", md: "1.25rem" } }}
                                >
                                    A distributed key-value NoSQL database that uses RocksDB as
                                    storage engine and is compatible with Redis protocol.
                                </Typography>

                                <div className="flex flex-wrap gap-4 pt-4">
                                    <Button
                                        variant="contained"
                                        size="large"
                                        className="transform rounded-full bg-gradient-to-r from-primary to-primary-dark px-8 py-3 text-lg shadow-lg transition-all duration-300 will-change-transform hover:-translate-y-1 hover:shadow-xl"
                                        onClick={handleGetStarted}
                                        endIcon={<ArrowForwardIcon />}
                                    >
                                        Get Started
                                    </Button>

                                    <Button
                                        variant="outlined"
                                        size="large"
                                        className="transform rounded-full border-2 border-primary px-8 py-3 text-lg text-primary transition-all duration-300 will-change-transform hover:-translate-y-1 hover:bg-primary/5 dark:border-primary-light dark:text-primary-light"
                                        href="https://github.com/apache/kvrocks"
                                        target="_blank"
                                        startIcon={<GitHubIcon />}
                                    >
                                        GitHub
                                    </Button>
                                </div>

                                <div className="pt-8">
                                    <div className="flex items-center space-x-1">
                                        <div className="h-1 w-1 rounded-full bg-green-500"></div>
                                        <div className="h-1 w-1 rounded-full bg-green-500 opacity-75"></div>
                                        <div className="h-1 w-1 rounded-full bg-green-500 opacity-50"></div>
                                        <Typography
                                            variant="body2"
                                            className="ml-2 text-green-600 dark:text-green-400"
                                        >
                                            Compatible with Redis protocol
                                        </Typography>
                                    </div>
                                </div>
                            </Box>
                        </Grid>

                        <Grid
                            item
                            xs={12}
                            md={6}
                            className={`${isMobile ? "order-1" : "order-2"} flex justify-center transition-all delay-300 duration-700 ${isLoaded ? "translate-y-0 opacity-100" : "translate-y-8 opacity-0"}`}
                        >
                            <Box className="relative flex w-full max-w-md items-center justify-center py-8">
                                <div className="absolute inset-0 rotate-3 scale-105 transform rounded-3xl bg-gradient-to-r from-blue-100/50 to-indigo-100/50 blur-xl will-change-transform dark:from-blue-900/20 dark:to-indigo-900/20"></div>

                                <div className="relative z-10 transform transition-all duration-700 will-change-transform hover:rotate-2 hover:scale-105">
                                    <div className="w-full overflow-hidden rounded-2xl bg-white shadow-2xl dark:bg-gray-800">
                                        <div className="flex items-center bg-gray-100 px-4 py-2 dark:bg-gray-900">
                                            <div className="flex space-x-2">
                                                <div className="h-3 w-3 rounded-full bg-red-500"></div>
                                                <div className="h-3 w-3 rounded-full bg-yellow-500"></div>
                                                <div className="h-3 w-3 rounded-full bg-green-500"></div>
                                            </div>
                                            <div className="ml-3 flex items-center text-xs text-gray-500 dark:text-gray-400">
                                                <TerminalIcon fontSize="small" className="mr-1" />
                                                redis-cli
                                            </div>
                                        </div>

                                        <div className="relative min-h-[300px] bg-gray-900 p-4 font-mono text-sm text-gray-100">
                                            {terminalLines.map((line, index) => (
                                                <div
                                                    key={index}
                                                    className={`${line.className} whitespace-pre`}
                                                >
                                                    {index < cursorPosition.y
                                                        ? line.text
                                                        : index === cursorPosition.y
                                                          ? line.text.substring(0, cursorPosition.x)
                                                          : ""}
                                                    {index === cursorPosition.y &&
                                                        cursorVisible &&
                                                        cursorPosition.x <= line.text.length && (
                                                            <span className="ml-0.5 inline-block h-4 w-2 animate-pulse bg-white"></span>
                                                        )}
                                                </div>
                                            ))}
                                        </div>
                                    </div>

                                    <div className="animate-float absolute -bottom-5 -right-5 rotate-12 transform">
                                        <Image
                                            src={mainImage}
                                            alt="Kvrocks Logo"
                                            width={120}
                                            height={120}
                                            className="drop-shadow-xl will-change-transform"
                                            style={{ width: "auto", height: "auto" }}
                                            priority
                                        />
                                    </div>
                                </div>
                            </Box>
                        </Grid>
                    </Grid>

                    <div className="absolute -bottom-20 left-1/2 hidden -translate-x-1/2 transform animate-bounce md:block">
                        <div className="flex flex-col items-center opacity-70 transition-opacity hover:opacity-100">
                            <Typography
                                variant="caption"
                                className="mb-1 text-gray-500 dark:text-gray-400"
                            >
                                Scroll to explore
                            </Typography>
                            <svg
                                width="24"
                                height="24"
                                viewBox="0 0 24 24"
                                fill="none"
                                xmlns="http://www.w3.org/2000/svg"
                            >
                                <path
                                    d="M12 5V19M12 19L19 12M12 19L5 12"
                                    stroke="currentColor"
                                    strokeWidth="2"
                                    strokeLinecap="round"
                                    strokeLinejoin="round"
                                />
                            </svg>
                        </div>
                    </div>
                </Container>
            </section>

            <section
                className="relative overflow-hidden bg-white py-24 will-change-transform dark:bg-dark-paper"
                style={featureSectionStyle}
            >
                <div className="absolute inset-0 -z-10">
                    <div className="absolute inset-0 bg-gradient-to-b from-gray-50 to-white dark:from-dark/50 dark:to-dark-paper"></div>
                </div>

                <Container maxWidth="lg" className="relative z-10">
                    <Box className="mb-16 text-center">
                        <div className="mb-4 inline-block rounded-lg bg-primary/10 px-4 py-1.5 text-xs font-semibold uppercase tracking-wide text-primary dark:bg-primary-dark/20 dark:text-primary-light">
                            Powerful Features
                        </div>

                        <Typography
                            variant="h2"
                            className="mb-4 font-bold text-gray-900 dark:text-gray-100"
                            sx={{ fontSize: { xs: "2rem", sm: "2.5rem", md: "3rem" } }}
                        >
                            Why Choose Kvrocks?
                        </Typography>

                        <Typography
                            variant="body1"
                            className="mx-auto mt-6 max-w-2xl text-lg text-gray-600 dark:text-gray-300"
                        >
                            Apache Kvrocks offers powerful capabilities that make it an excellent
                            choice for your database needs
                        </Typography>
                    </Box>

                    <Grid container spacing={4}>
                        {features.map((feature, index) => (
                            <Grid item xs={12} sm={6} md={3} key={index}>
                                <Card
                                    elevation={0}
                                    className={`h-full overflow-hidden rounded-xl border border-gray-100 bg-white/90 backdrop-blur-sm transition-all duration-500 hover:-translate-y-2 hover:border-primary/30 hover:shadow-xl dark:border-gray-800 dark:bg-dark-paper/90 dark:hover:border-primary-light/30 ${isLoaded ? "translate-y-0 opacity-100" : "translate-y-12 opacity-0"}`}
                                    style={{
                                        transitionDelay: `${feature.delay}ms`,
                                        transform: isLoaded
                                            ? "translateY(0) scale(1)"
                                            : "translateY(40px) scale(0.95)",
                                        willChange: "transform, opacity",
                                    }}
                                >
                                    <CardContent className="flex h-full flex-col items-center p-6 text-center">
                                        <div
                                            className="mb-6 flex h-16 w-16 items-center justify-center rounded-2xl text-white shadow-lg transition-transform duration-200 will-change-transform hover:scale-110"
                                            style={{ backgroundColor: feature.color }}
                                        >
                                            {feature.icon}
                                        </div>

                                        <Typography
                                            variant="h5"
                                            className="mb-4 font-bold text-gray-900 dark:text-gray-100"
                                        >
                                            {feature.title}
                                        </Typography>

                                        <Typography
                                            variant="body1"
                                            className="text-gray-600 dark:text-gray-300"
                                        >
                                            {feature.description}
                                        </Typography>
                                    </CardContent>
                                </Card>
                            </Grid>
                        ))}
                    </Grid>
                </Container>
            </section>

            <section className="dark:to-dark-800 relative overflow-hidden bg-gradient-to-br from-gray-50 to-blue-50 py-24 dark:from-dark">
                <div className="absolute inset-0 -z-10 overflow-hidden opacity-30 dark:opacity-10">
                    <svg viewBox="0 0 1000 1000" xmlns="http://www.w3.org/2000/svg">
                        <defs>
                            <pattern
                                id="circlePattern"
                                x="0"
                                y="0"
                                width="20"
                                height="20"
                                patternUnits="userSpaceOnUse"
                            >
                                <circle cx="10" cy="10" r="1" fill="currentColor" />
                            </pattern>
                        </defs>
                        <rect width="100%" height="100%" fill="url(#circlePattern)" />
                    </svg>
                </div>

                <Container maxWidth="lg" className="relative z-10">
                    <Box className="mb-16 text-center">
                        <div className="mb-4 inline-block rounded-lg bg-secondary/10 px-4 py-1.5 text-xs font-semibold uppercase tracking-wide text-secondary dark:bg-secondary-dark/20 dark:text-secondary-light">
                            Powerful Management
                        </div>

                        <Typography
                            variant="h2"
                            className="mb-4 font-bold text-gray-900 dark:text-gray-100"
                            sx={{ fontSize: { xs: "2rem", sm: "2.5rem", md: "3rem" } }}
                        >
                            About Kvrocks Controller
                        </Typography>
                    </Box>

                    <Card
                        elevation={0}
                        className="overflow-hidden rounded-3xl border border-gray-100 bg-white/80 shadow-2xl backdrop-blur-sm will-change-transform dark:border-gray-800 dark:bg-dark-paper/80"
                    >
                        <CardContent className="p-0">
                            <Grid container sx={{ minHeight: "500px" }}>
                                <Grid item xs={12} md={6}>
                                    <Box className="flex h-full flex-col justify-center p-8 md:p-12">
                                        <Typography
                                            variant="h3"
                                            className="mb-6 font-bold text-gray-900 dark:text-gray-100"
                                        >
                                            Simplified Management
                                        </Typography>

                                        <Typography
                                            variant="body1"
                                            className="mb-8 text-gray-700 dark:text-gray-300"
                                        >
                                            Kvrocks Controller provides a web management interface
                                            for Apache Kvrocks clusters, enabling efficient
                                            distribution, monitoring, and maintenance of your Redis
                                            compatible database infrastructure.
                                        </Typography>

                                        <div className="space-y-6">
                                            {benefits.map((benefit, index) => (
                                                <div
                                                    key={index}
                                                    className="flex items-start gap-4 rounded-xl border border-gray-100 bg-gray-50 p-4 transition-all will-change-transform hover:border-primary/30 hover:shadow-md dark:border-gray-800 dark:bg-dark/50"
                                                >
                                                    <div className="rounded-lg bg-primary/10 p-3 text-primary dark:bg-primary-dark/20 dark:text-primary-light">
                                                        {benefit.icon}
                                                    </div>
                                                    <div>
                                                        <Typography
                                                            variant="subtitle1"
                                                            className="font-semibold text-gray-900 dark:text-gray-100"
                                                        >
                                                            {benefit.title}
                                                        </Typography>
                                                        <Typography
                                                            variant="body2"
                                                            className="text-gray-600 dark:text-gray-400"
                                                        >
                                                            {benefit.description}
                                                        </Typography>
                                                    </div>
                                                </div>
                                            ))}
                                        </div>
                                    </Box>
                                </Grid>

                                <Grid
                                    item
                                    xs={12}
                                    md={6}
                                    className="bg-gradient-to-br from-gray-900 to-gray-800 text-white dark:from-gray-900 dark:to-gray-800"
                                >
                                    <Box className="flex h-full flex-col p-8 md:p-12">
                                        <div className="mb-4 flex items-center">
                                            <div className="flex space-x-2">
                                                <div className="h-3 w-3 rounded-full bg-red-500"></div>
                                                <div className="h-3 w-3 rounded-full bg-yellow-500"></div>
                                                <div className="h-3 w-3 rounded-full bg-green-500"></div>
                                            </div>
                                            <div className="ml-4 flex items-center text-xs text-gray-400">
                                                <TerminalIcon fontSize="small" className="mr-1" />
                                                Terminal
                                            </div>
                                        </div>

                                        <div className="relative flex-1 overflow-hidden rounded-xl bg-gray-950 p-6 font-mono text-sm">
                                            <div className="space-y-2">
                                                <div className="text-green-400">
                                                    $ redis-cli -p 6666
                                                </div>
                                                <div className="text-blue-400">
                                                    127.0.0.1:6666&gt; SET mykey &quot;Hello
                                                    Kvrocks&quot;
                                                </div>
                                                <div className="text-yellow-400">OK</div>
                                                <div className="text-blue-400">
                                                    127.0.0.1:6666&gt; GET mykey
                                                </div>
                                                <div className="text-yellow-400">
                                                    &quot;Hello Kvrocks&quot;
                                                </div>
                                                <div className="text-blue-400">
                                                    127.0.0.1:6666&gt; INFO
                                                </div>
                                                <div className="text-yellow-200"># Server</div>
                                                <div className="text-yellow-400">
                                                    redis_version:6.0.0
                                                </div>
                                                <div className="text-yellow-400">
                                                    kvrocks_version:2.0.0
                                                </div>
                                                <div className="text-blue-400">
                                                    127.0.0.1:6666&gt; KEYS *
                                                </div>
                                                <div className="text-yellow-400">
                                                    1) &quot;mykey&quot;
                                                </div>
                                                <div className="text-blue-400">
                                                    127.0.0.1:6666&gt; _
                                                </div>
                                            </div>

                                            <div
                                                className={`cursor-blink absolute bottom-6 right-6 h-4 w-2 bg-white`}
                                                style={{
                                                    opacity: cursorVisible ? 1 : 0,
                                                    transition: "opacity 0.3s ease",
                                                }}
                                            ></div>
                                        </div>

                                        <div className="mt-6 flex items-center justify-between">
                                            <Typography variant="caption" className="text-gray-400">
                                                Compatible with Redis protocol
                                            </Typography>
                                            <Chip
                                                icon={<CheckCircleOutlineIcon fontSize="small" />}
                                                label="Ready"
                                                size="small"
                                                color="success"
                                                className="bg-success/80 transition-colors hover:bg-success"
                                            />
                                        </div>
                                    </Box>
                                </Grid>
                            </Grid>
                        </CardContent>
                    </Card>
                </Container>
            </section>

            <section className="relative overflow-hidden bg-white py-24 dark:bg-dark-paper">
                <div className="absolute inset-0 -z-10">
                    <div className="absolute inset-0 bg-gradient-to-t from-gray-50 to-white dark:from-dark-paper/70 dark:to-dark"></div>
                </div>

                <Container maxWidth="lg" className="relative z-10">
                    <Box className="mb-16 text-center">
                        <Typography
                            variant="h2"
                            className="mb-4 font-bold text-gray-900 dark:text-gray-100"
                            sx={{ fontSize: { xs: "2rem", sm: "2.5rem", md: "3rem" } }}
                        >
                            Resources
                        </Typography>

                        <Typography
                            variant="body1"
                            className="mx-auto mt-6 max-w-2xl text-lg text-gray-600 dark:text-gray-300"
                        >
                            Explore documentation and source code to get the most out of Kvrocks
                            Controller
                        </Typography>
                    </Box>

                    <Grid container spacing={4} justifyContent="center">
                        {resources.map((resource, index) => (
                            <Grid item xs={12} sm={6} md={5} key={index}>
                                <Link
                                    href={resource.url}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    className="block h-full"
                                >
                                    <Card
                                        elevation={0}
                                        className="h-full overflow-hidden rounded-2xl border border-gray-100 bg-white/90 backdrop-blur-sm transition-all duration-500 will-change-transform hover:-translate-y-2 hover:border-primary/30 hover:shadow-xl dark:border-gray-800 dark:bg-dark-paper/90 dark:hover:border-primary-light/30"
                                        style={{ transitionDelay: `${index * 200}ms` }}
                                    >
                                        <CardContent className="p-8">
                                            <div
                                                className="mb-6 flex h-14 w-14 items-center justify-center rounded-2xl"
                                                style={{
                                                    background: resource.color,
                                                    color: "white",
                                                }}
                                            >
                                                {React.cloneElement(resource.icon, {
                                                    sx: { fontSize: 28 },
                                                })}
                                            </div>

                                            <Typography
                                                variant="h5"
                                                className="mb-4 flex items-center font-bold text-gray-900 dark:text-gray-100"
                                            >
                                                {resource.title}
                                                <LaunchIcon
                                                    fontSize="small"
                                                    className="ml-2 text-gray-400"
                                                />
                                            </Typography>

                                            <Typography
                                                variant="body1"
                                                className="text-gray-600 dark:text-gray-300"
                                            >
                                                {resource.description}
                                            </Typography>
                                        </CardContent>
                                    </Card>
                                </Link>
                            </Grid>
                        ))}
                    </Grid>
                </Container>
            </section>

            <section className="relative overflow-hidden py-20">
                <div className="absolute inset-0 -z-10">
                    <div className="absolute inset-0 bg-gradient-to-br from-primary/5 via-indigo-50 to-white dark:from-primary-dark/10 dark:via-indigo-950/5 dark:to-dark"></div>

                    <div className="absolute left-1/4 top-1/4 h-64 w-64 rounded-full bg-blue-400/30 blur-3xl will-change-transform dark:bg-blue-400/10"></div>
                    <div className="absolute bottom-1/4 right-1/4 h-64 w-64 rounded-full bg-indigo-400/30 blur-3xl will-change-transform dark:bg-indigo-400/10"></div>
                </div>

                <Container maxWidth="md" className="relative z-10">
                    <Card
                        elevation={0}
                        className="overflow-hidden rounded-3xl border-0 bg-gradient-to-br from-white/80 to-blue-50/80 shadow-2xl backdrop-blur-md will-change-transform dark:from-gray-900/80 dark:to-indigo-950/80"
                    >
                        <CardContent className="p-12 text-center">
                            <Typography
                                variant="h3"
                                className="mb-6 font-bold text-gray-900 dark:text-gray-100"
                            >
                                Ready to experience the power of Apache Kvrocks?
                            </Typography>

                            <Typography
                                variant="body1"
                                className="mx-auto mb-10 max-w-2xl text-gray-700 dark:text-gray-300"
                            >
                                Start managing your distributed database infrastructure with our
                                intuitive and powerful controller interface.
                            </Typography>

                            <Button
                                variant="contained"
                                size="large"
                                className="transform rounded-full bg-gradient-to-r from-primary to-primary-dark px-10 py-3 text-lg shadow-lg transition-all duration-300 will-change-transform hover:-translate-y-1 hover:shadow-xl"
                                onClick={handleGetStarted}
                                endIcon={<ArrowForwardIcon />}
                            >
                                Get Started Now
                            </Button>
                        </CardContent>
                    </Card>
                </Container>
            </section>
        </div>
    );
}
