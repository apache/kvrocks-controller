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
import GitHubIcon from "@mui/icons-material/GitHub";
import TwitterIcon from "@mui/icons-material/Twitter";
import LinkedInIcon from "@mui/icons-material/LinkedIn";
import LaunchIcon from "@mui/icons-material/Launch";
import { footerConfig } from "../../../config";
import { footerColumn, footerColumnItem } from "../lib/definitions";

const socials = [
    { icon: <GitHubIcon sx={{ fontSize: 15 }} />, href: "https://github.com/apache", label: "GitHub" },
    { icon: <TwitterIcon sx={{ fontSize: 15 }} />, href: "https://x.com/TheASF", label: "Twitter" },
    {
        icon: <LinkedInIcon sx={{ fontSize: 15 }} />,
        href: "https://www.linkedin.com/company/the-apache-software-foundation/",
        label: "LinkedIn",
    },
];

const Column = ({ column }: { column: footerColumn }) => (
    <div>
        <div className="lin-eyebrow mb-3">{column.title}</div>
        <ul className="space-y-2">
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
            className="inline-flex items-center gap-1 text-sm text-text-secondary transition-colors hover:text-text-primary dark:text-text-dark-secondary dark:hover:text-text-dark-primary"
        >
            {item.label}
            {item.href && <LaunchIcon sx={{ fontSize: 11 }} className="opacity-60" />}
        </Link>
    </li>
);

export default function Footer() {
    return (
        <footer className="mt-auto border-t border-border-subtle bg-surface-subtle dark:border-border-dark-subtle dark:bg-surface-dark-subtle">
            <div className="mx-auto max-w-[1440px] px-4 py-10">
                <div className="grid gap-8 md:grid-cols-[1fr_2fr]">
                    <div className="space-y-4">
                        <Link href="/" className="inline-flex items-center gap-2 no-underline">
                            <Image
                                src="/logo.svg"
                                width={20}
                                height={20}
                                alt="Apache Kvrocks"
                                style={{ height: "auto" }}
                            />
                            <span className="text-sm font-semibold text-text-primary dark:text-text-dark-primary">
                                Kvrocks Controller
                            </span>
                        </Link>
                        <p className="max-w-xs text-sm text-text-muted dark:text-text-dark-muted">
                            Distributed key-value store built on RocksDB, compatible with the Redis
                            protocol.
                        </p>
                        <div className="flex gap-1">
                            {socials.map((social) => (
                                <a
                                    key={social.label}
                                    href={social.href}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    aria-label={social.label}
                                    className="flex h-7 w-7 items-center justify-center rounded-md text-text-muted transition-colors hover:bg-surface-hover hover:text-text-primary dark:text-text-dark-muted dark:hover:bg-surface-dark-hover dark:hover:text-text-dark-primary"
                                >
                                    {social.icon}
                                </a>
                            ))}
                        </div>
                    </div>

                    <div className="grid grid-cols-2 gap-6 sm:grid-cols-3">
                        {footerConfig.links.map((column) => (
                            <Column key={column.title} column={column} />
                        ))}
                    </div>
                </div>

                <div className="mt-10 flex flex-col items-start justify-between gap-4 border-t border-border-subtle pt-6 dark:border-border-dark-subtle sm:flex-row sm:items-center">
                    <a
                        href={footerConfig.logo.href}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="flex items-center gap-2"
                    >
                        <Image
                            src={footerConfig.logo.src}
                            height={20}
                            width={50}
                            alt={footerConfig.logo.alt}
                            className="opacity-70"
                            style={{ width: "auto", height: "auto" }}
                        />
                        <span className="text-xs text-text-muted dark:text-text-dark-muted">
                            An Apache Software Foundation project
                        </span>
                    </a>
                    <span className="max-w-xl text-xs leading-relaxed text-text-muted dark:text-text-dark-muted sm:text-right">
                        {`© ${new Date().getFullYear()} ${footerConfig.copyright}`}
                    </span>
                </div>
            </div>
        </footer>
    );
}
