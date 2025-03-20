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
import { usePathname } from "next/navigation";

const links = [
    {
        url: '/',
        title: 'Home'
    },{
        url: '/namespaces',
        title: 'Namespaces'
    },{
        url: 'https://kvrocks.apache.org',
        title: 'Documentation',
        _blank: true
    },
];

export default function Banner() {
    const { isDarkMode, toggleTheme } = useTheme();
    const pathname = usePathname();
    
    // Generate breadcrumb from pathname
    const breadcrumbs = pathname.split('/').filter(Boolean);
    
    return (
        <AppBar position="fixed" elevation={1} className="bg-white dark:bg-dark-paper text-gray-800 dark:text-gray-100">
            <Container maxWidth={false}>
                <Toolbar className="flex justify-between">
                    <div className="flex items-center">
                        <Image src="/logo.svg" width={40} height={40} alt='logo' className="mr-4" />
                        <Typography variant="h6" component="div" className="hidden sm:block font-medium text-primary dark:text-primary-light">
                            Apache Kvrocks Controller
                        </Typography>
                    </div>
                    
                    <Box className="hidden md:flex items-center space-x-1">
                        <NavLinks links={links} />
                    </Box>
                    
                    <Box className="flex items-center">
                        {breadcrumbs.length > 0 && (
                            <Box className="hidden md:flex items-center text-sm px-4 py-1 bg-gray-100 dark:bg-dark-border rounded-md mr-4">
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