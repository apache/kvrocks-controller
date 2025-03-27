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

import Image from "next/image";
import Link from "next/link";
import LaunchIcon from "@mui/icons-material/Launch";
import { footerConfig } from "../../../config";
import { footerColumn, footerColumnItem, footerLogo } from "../lib/definitions";

export default function Footer() {
    return (
        <footer className="box-border w-full bg-[#303846] py-2 py-8 text-[#ebedf0] lg:px-8">
            <div className="flex flex-col px-4">
                <div className="grid grid-cols-3 justify-items-start gap-2">
                    {footerConfig.links.map((column) => (
                        <Column key={column.title} column={column} />
                    ))}
                    <div className="col-span-full mt-px flex w-full flex-col items-center text-center">
                        <Logo logo={footerConfig.logo} />
                        <Copyright copyright={footerConfig.copyright} />
                    </div>
                </div>
            </div>
        </footer>
    );
}

const Column = ({ column }: { column: footerColumn }) => (
    <div className="flex w-full flex-col items-start px-4">
        <span className="mb-1 min-h-10 font-bold">{column.title}</span>
        {column.items.map((item) => (
            <LinkItem key={item.label} item={item} />
        ))}
    </div>
);

const LinkItem = ({ item }: { item: footerColumnItem }) => (
    <div className="font-(serif:Avenir-Medium) flex min-h-8 flex-col font-light">
        <Link href={item.href || item.to || "/"} target={item.href && "_blank"}>
            <div className="inline-flex hover:text-sky-700 hover:underline">
                {item.label}
                <span>{item.href && <LaunchIcon className="mx-px size-4" />}</span>
            </div>
        </Link>
    </div>
);

const Copyright = ({ copyright }: { copyright: string }) => (
    <div className="font-(serif:Avenir-Medium) flex min-h-16 w-full items-center border-t border-[#ccc] text-center text-sm leading-[20px] text-[#999]">
        <span>{`Copyright © ${new Date().getFullYear()} ${copyright}`}</span>
    </div>
);

const Logo = ({ logo }: { logo: footerLogo }) => (
    <div className="mb-2 box-border flex w-full place-content-center">
        <Link href={logo.href} className="opacity-50 transition-opacity hover:opacity-100">
            <Image
                src={logo.src}
                height={logo.height}
                width={logo.width}
                alt={logo.alt}
                className="mt-4"
            />
        </Link>
    </div>
);
