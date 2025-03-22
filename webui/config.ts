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

import { footerConfigType } from "@/app/lib/definitions";

export const footerConfig: footerConfigType = {
    links: [
        {
            title: 'Docs',
            items: [
                {
                    label: 'Getting started',
                    to: '/docs/getting-started',
                },
                {
                    label: 'Supported commands',
                    to: '/docs/supported-commands',
                },
                {
                    label: 'How to contribute',
                    to: '/community/contributing',
                }
            ],
        },
        {
            title: 'Community',
            items: [
                {
                    label: 'Zulip',
                    href: 'https://kvrocks.zulipchat.com/',
                },
                {
                    label: 'Issue Tracker',
                    href: 'https://github.com/apache/kvrocks-controller/issues',
                },
                {
                    label: 'Mailing list',
                    href: 'https://lists.apache.org/list.html?dev@kvrocks.apache.org',
                },
            ],
        },
        {
            title: 'Repositories',
            items: [
                {
                    label: 'Kvrocks',
                    href: 'https://github.com/apache/kvrocks',
                },
                {
                    label: 'Website',
                    href: 'https://github.com/apache/kvrocks-website',
                },
                {
                    label: 'Controller',
                    href: 'https://github.com/apache/kvrocks-controller',
                }
            ]
        }
    ],
    logo: {
        height: 128,
        width: 320,
        alt: 'Apache logo',
        src: '/asf_logo.svg',
        href: 'https://www.apache.org/'
    },
    copyright: `The Apache Software Foundation. Apache Kvrocks, Kvrocks, and its feather logo are trademarks of The Apache Software Foundation. Redis and its cube logo are registered trademarks of Redis Ltd.`,
};