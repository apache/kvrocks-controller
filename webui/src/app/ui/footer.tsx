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
import LaunchIcon from '@mui/icons-material/Launch';
import { footerConfig } from "../../../config";
import { footerColumn, footerColumnItem, footerLogo } from "../lib/definitions";

export default function Footer(){
    return (
        <footer className="bg-[#303846] text-[#ebedf0] py-2 w-full box-border lg:px-8 py-8">
            <div className="flex flex-col px-4">
                <div className="grid grid-cols-3 gap-2 justify-items-start">
                    {footerConfig.links.map(column => <Column key={column.title} column={column}/>)}
                    <div className="col-span-full mt-px flex flex-col items-center text-center w-full">
                        <Logo logo={footerConfig.logo}/>
                        <Copyright copyright={footerConfig.copyright}/>
                    </div>
                </div>
            </div>
        </footer>
    );
}

const Column = ({column }: {column : footerColumn}) =>
    <div className="flex flex-col w-full items-start px-4">
        <span className="mb-1 font-bold min-h-10">{column.title}</span>
        {column.items.map(item => <LinkItem key={item.label} item={item} />)}
    </div>

const LinkItem = ({item} : { item: footerColumnItem}) =>
    <div className="flex flex-col min-h-8 font-(serif:Avenir-Medium) font-light">
        <Link href={item.href || item.to || '/'} target={item.href && "_blank"}>
            <div className="inline-flex hover:underline hover:text-sky-700">
                {item.label}
                <span>{item.href && <LaunchIcon className="mx-px size-4"/>}</span>
            </div>
        </Link>
    </div>

const Copyright = ({copyright} : {copyright: string}) =>
    <div className="text-center flex items-center border-t border-[#ccc] min-h-16 w-full leading-[20px] font-(serif:Avenir-Medium) text-sm text-[#999]">
        <span>{`Copyright © ${new Date().getFullYear()} ${copyright}`}</span>
    </div>

const Logo = ({logo} : { logo: footerLogo}) =>
    <div className="mb-2 box-border w-full flex place-content-center">
        <Link href={logo.href} className="opacity-50 hover:opacity-100 transition-opacity">
            <Image
                src={logo.src}
                height={logo.height}
                width={logo.width}
                alt={logo.alt}
                className="mt-4"
            />
        </Link>
    </div>
