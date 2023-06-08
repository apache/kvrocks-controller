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
import { ConfigProvider, Layout, theme as antdTheme } from 'antd';
import './App.css';
import { Sidebar } from './components/sidebar/Sidebar';
import { useNavigate, useRoutes } from 'react-router-dom';
import { router } from './router';
import { useLocalstorage } from './hooks/useLocalStorage';
import { createContext, useCallback, useState } from 'react';
import { NamespaceCreationModal } from './components/sidebar/NamespaceCreationModal';
import { emptyFunction } from './common/utils';
import { Header } from './components/Header';

export const NamespaceCreationDialogDisplayContext = createContext<() => void>(emptyFunction);
export const ClusterCreationDialogDisplayContext = createContext<() => void>(emptyFunction);
export const ThemeSwitchContext = createContext<[string, () => void]>(['', emptyFunction]);
function App() {
    const routerElement = useRoutes(router);
    const [theme, setTheme] = useLocalstorage<'light'|'dark'>('theme', 'light');
    const changeTheme = useCallback(() => {
        setTheme(theme == 'dark' ? 'light' : 'dark');
    },[theme]);

    const navigate = useNavigate();
    const [nsCreationModal, setNsCreationModal] = useState(false);
    const onNamespaceCreated = useCallback((name: string) => {
        navigate(`/${name}`);
    }, [navigate]);
    return (
        <ConfigProvider theme={{algorithm: theme == 'dark' ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm}}>
            <NamespaceCreationDialogDisplayContext.Provider value={() => setNsCreationModal(true)}>
                <Layout>
                    <ThemeSwitchContext.Provider value={[theme, changeTheme]}>
                        <Header></Header>
                    </ThemeSwitchContext.Provider>
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
            </NamespaceCreationDialogDisplayContext.Provider>
            <NamespaceCreationModal 
                open={nsCreationModal} 
                onclose={() => setNsCreationModal(false)}
                oncreated={onNamespaceCreated}
            />
        </ConfigProvider>
    );
}

export default App;
