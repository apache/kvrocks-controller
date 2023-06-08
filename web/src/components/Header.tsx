
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
 *
 */
import { Button, Layout, Space } from 'antd';
import { useContext } from 'react';
import { NamespaceCreationDialogDisplayContext, ThemeSwitchContext } from '../App';
import { BulbOutlined, GithubOutlined } from '@ant-design/icons';
import { Logo } from './Logo';

const githubUrl = 'https://github.com/apache/incubator-kvrocks';

export function Header() {
    const [theme, changeTheme] = useContext(ThemeSwitchContext);
    const openNamespaceCreation = useContext(NamespaceCreationDialogDisplayContext);
    return (<Layout.Header style={{
        backgroundColor: theme == 'light' ? 'white' : '',
        display: 'flex',
        justifyContent: 'space-between'
    }}>
        <Logo></Logo>
        <div style={{fontSize: '20px'}}>
            <Space size='large'>
                <Button onClick={openNamespaceCreation}>Create Namespace</Button>
                <BulbOutlined onClick={changeTheme} style={{cursor: 'pointer'}}/>
                <GithubOutlined onClick={() => window.open(githubUrl)} style={{cursor: 'pointer'}}/>
            </Space>
        </div>
    </Layout.Header>);
}