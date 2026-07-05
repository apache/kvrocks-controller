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

import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";
import Banner from "./ui/banner";
import { ThemeProvider } from "./theme-provider";
import Footer from "./ui/footer";
import Breadcrumb from "./ui/breadcrumb";
import SpotlightSearch from "./ui/spotlight-search";

const inter = Inter({ subsets: ["latin"], display: "swap" });

export const metadata: Metadata = {
    title: "Apache Kvrocks Controller",
    description: "Management UI for Apache Kvrocks clusters",
};

export default function RootLayout({
    children,
}: Readonly<{
    children: React.ReactNode;
}>) {
    return (
        <html lang="en" suppressHydrationWarning>
            <body
                className={`${inter.className} min-h-screen bg-surface-base text-text-primary antialiased dark:bg-surface-dark-base dark:text-text-dark-primary`}
                suppressHydrationWarning
            >
                <ThemeProvider>
                    <SpotlightSearch />
                    <Banner />
                    <div className="flex min-h-screen flex-col pt-[var(--lin-topbar-height)]">
                        <Breadcrumb />
                        <main className="flex-1">{children}</main>
                        <Footer />
                    </div>
                </ThemeProvider>
            </body>
        </html>
    );
}
