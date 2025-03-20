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

import { Button, Typography, Box, Paper, Grid } from "@mui/material";
import Image from "next/image";
import { useRouter } from "next/navigation";
import { useTheme } from "./theme-provider";
import StorageIcon from '@mui/icons-material/Storage';
import DnsIcon from '@mui/icons-material/Dns';
import DeviceHubIcon from '@mui/icons-material/DeviceHub';
import BarChartIcon from '@mui/icons-material/BarChart';
import GitHubIcon from '@mui/icons-material/GitHub';
import LaunchIcon from '@mui/icons-material/Launch';
import MenuBookIcon from '@mui/icons-material/MenuBook';
import Link from "next/link";

export default function Home() {
    const router = useRouter();
    const { isDarkMode } = useTheme();
    const currentYear = new Date().getFullYear(); // minor change: compute current year once

    const features = [
        {
            title: "Cluster Management",
            description: "Create, modify, and monitor Redis clusters with an intuitive interface",
            icon: <StorageIcon sx={{ fontSize: 40 }} className="text-primary dark:text-primary-light" />
        },
        {
            title: "Shard Distribution",
            description: "Efficiently distribute data across multiple shards for optimal performance",
            icon: <DnsIcon sx={{ fontSize: 40 }} className="text-primary dark:text-primary-light" />
        },
        {
            title: "Node Monitoring",
            description: "Monitor node health, performance, and connectivity in real-time",
            icon: <DeviceHubIcon sx={{ fontSize: 40 }} className="text-primary dark:text-primary-light" />
        },
        {
            title: "Advanced Metrics",
            description: "View detailed performance metrics to optimize your infrastructure",
            icon: <BarChartIcon sx={{ fontSize: 40 }} className="text-primary dark:text-primary-light" />
        }
    ];

    const resources = [
        {
            title: "Documentation",
            description: "Learn how to use Kvrocks Controller",
            icon: <MenuBookIcon sx={{ fontSize: 30 }} />,
            url: "https://kvrocks.apache.org/docs/"
        },
        {
            title: "GitHub Repository",
            description: "View the source code on GitHub",
            icon: <GitHubIcon sx={{ fontSize: 30 }} />,
            url: "https://github.com/apache/kvrocks-controller"
        }
    ];

    return (
        <div className="flex flex-col min-h-[calc(100vh-64px)] bg-gradient-to-b from-white to-gray-50 dark:from-dark dark:to-dark-paper">
            {/* Hero Section */}
            <section className="flex-grow flex flex-col items-center justify-center px-6 py-12 text-center">
                <div className="max-w-3xl mx-auto">
                    <div className="mb-8 mx-auto relative w-40 h-40">
                        <Image
                            src="/logo.svg"
                            alt="Kvrocks Logo"
                            layout="fill"
                            objectFit="contain"
                            priority
                            className="animate-[pulse_4s_ease-in-out_infinite]"
                        />
                    </div>
                    
                    <Typography variant="h2" component="h1" className="font-bold mb-4 text-gray-900 dark:text-gray-100">
                        Apache Kvrocks <span className="text-primary dark:text-primary-light">Controller</span>
                    </Typography>
                    
                    <Typography variant="h6" className="mb-8 text-gray-600 dark:text-gray-300 max-w-2xl mx-auto">
                        A web management interface for Apache Kvrocks clusters, enabling efficient distribution, monitoring, and maintenance of your Redis compatible database infrastructure.
                    </Typography>
                    
                    <div className="flex flex-wrap justify-center gap-4">
                        <Button 
                            variant="contained" 
                            size="large"
                            className="bg-primary hover:bg-primary-dark px-8 py-3 text-lg"
                            onClick={() => router.push('/namespaces')}
                        >
                            Get Started
                        </Button>
                        
                        <Button 
                            variant="outlined" 
                            size="large"
                            className="border-primary text-primary hover:bg-primary hover:text-white dark:border-primary-light dark:text-primary-light px-8 py-3 text-lg"
                            href="https://github.com/apache/kvrocks-controller/issues"
                            target="_blank"
                        >
                            Submit Feedback
                        </Button>
                    </div>
                </div>
            </section>

            {/* Features Section */}
            <section className="py-16 px-6 bg-gray-50 dark:bg-dark-paper">
                <div className="max-w-6xl mx-auto">
                    <Typography variant="h4" component="h2" className="text-center font-bold mb-12 text-gray-900 dark:text-gray-100">
                        Key Features
                    </Typography>
                    
                    <Grid container spacing={4}>
                        {features.map((feature, index) => (
                            <Grid item xs={12} sm={6} md={3} key={index}>
                                <Paper 
                                    elevation={0}
                                    className="card h-full p-6 flex flex-col items-center text-center"
                                >
                                    <div className="mb-4">
                                        {feature.icon}
                                    </div>
                                    <Typography variant="h6" className="mb-2 font-medium">
                                        {feature.title}
                                    </Typography>
                                    <Typography variant="body2" className="text-gray-600 dark:text-gray-300">
                                        {feature.description}
                                    </Typography>
                                </Paper>
                            </Grid>
                        ))}
                    </Grid>
                </div>
            </section>

            {/* Resources Section */}
            <section className="py-16 px-6">
                <div className="max-w-4xl mx-auto">
                    <Typography variant="h4" component="h2" className="text-center font-bold mb-12 text-gray-900 dark:text-gray-100">
                        Resources
                    </Typography>
                    
                    <Grid container spacing={4} justifyContent="center">
                        {resources.map((resource, index) => (
                            <Grid item xs={12} sm={6} key={index}>
                                <Link href={resource.url} target="_blank" rel="noopener noreferrer">
                                    <Paper 
                                        elevation={0}
                                        className="card h-full p-6 flex flex-col hover:border-primary dark:hover:border-primary-light transition-all"
                                    >
                                        <div className="flex items-center mb-4">
                                            <div className="p-2 rounded-full bg-primary/10 dark:bg-primary-dark/20 mr-4">
                                                {resource.icon}
                                            </div>
                                            <div>
                                                <Typography variant="h6" className="flex items-center">
                                                    {resource.title} 
                                                    <LaunchIcon fontSize="small" className="ml-2 text-gray-400" />
                                                </Typography>
                                                <Typography variant="body2" className="text-gray-600 dark:text-gray-300">
                                                    {resource.description}
                                                </Typography>
                                            </div>
                                        </div>
                                    </Paper>
                                </Link>
                            </Grid>
                        ))}
                    </Grid>
                </div>
            </section>

            {/* Footer */}
            <footer className="py-6 px-6 border-t border-light-border dark:border-dark-border">
                <div className="max-w-6xl mx-auto text-center">
                    <Typography variant="body2" className="text-gray-500 dark:text-gray-400">
                        Copyright © {currentYear} The Apache Software Foundation. Apache Kvrocks, Kvrocks, and its feather logo are trademarks of The Apache Software Foundation. Redis and its cube logo are registered trademarks of Redis Ltd. Apache Kvrocks Controller is released under Apache License, Version 2.0.
                    </Typography>
                </div>
            </footer>
        </div>
    );
}