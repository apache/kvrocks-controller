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
import { ConfigProvider, Layout, Space, theme as antdTheme } from 'antd';
import { Logo } from './components/Logo';
import './App.css';
import { Sidebar } from './components/sidebar/Sidebar';
import { useRoutes } from 'react-router-dom';
import { router } from './router';
import { BulbOutlined, GithubOutlined } from '@ant-design/icons';
import { useLocalstorage } from './hooks/useLocalStorage';
import { useCallback } from 'react';

const githubUrl = 'https://github.com/apache/incubator-kvrocks';
function App() {
    const routerElement = useRoutes(router);
    const [theme, setTheme] = useLocalstorage<'light'|'dark'>('theme', 'light');
    const changeTheme = useCallback(() => {
        setTheme(theme == 'dark' ? 'light' : 'dark');
    },[theme]);
    return (
        <ConfigProvider theme={{algorithm: theme == 'dark' ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm}}>
            <Layout>
                <Layout.Header style={{
                    backgroundColor: theme == 'light' ? 'white' : '',
                    display: 'flex',
                    justifyContent: 'space-between'
                }}>
                    <Logo></Logo>
                    <div style={{fontSize: '20px'}}>
                        <Space size='large'>
                            <BulbOutlined onClick={changeTheme} style={{cursor: 'pointer'}}/>
                            <GithubOutlined onClick={() => window.open(githubUrl)} style={{cursor: 'pointer'}}/>
                        </Space>
                    </div>
                </Layout.Header>
                <Layout hasSider style={{height: 'calc(100vh - 64px)'}}>
                    <Layout.Sider
                        theme={theme}
                    >
                        <Sidebar/>
                    </Layout.Sider>
                    <Layout.Content style={{padding: '20px', overflow: 'auto'}}>
                        {
                            routerElement
                        }
                    </Layout.Content>
                </Layout>
            </Layout>
        </ConfigProvider>
    );
}

export default App;
