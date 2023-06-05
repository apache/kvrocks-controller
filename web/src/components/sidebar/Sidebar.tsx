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
import { Button, Divider, Menu, MenuProps, Space, Spin } from 'antd';
import { useCallback, useEffect, useState } from 'react';
import { useApi } from '../../hooks/useApi';
import { SubMenuType } from 'antd/es/menu/hooks/useItems';
import { NamespaceCreationModal } from './NamespaceCreationModal';
import { ClusterCreationModal } from './ClusterCreationModal';
import { useLocation, useNavigate } from 'react-router-dom';

type MenuItem = Required<MenuProps>['items'][number];

export function Sidebar() {
    const { pathname: currentPath, state: locationState } = useLocation();
    useEffect(() => {
        if(locationState != 'NO_NEED_REFRESH_MENU') {
            refreshNamespace(undefined);
        }
        setSelectedKeys([currentPath]);
        setOpenKeys(currentPath.split('/').length == 3
            ?
            [currentPath.split('/').slice(0,2).join('/')]
            :
            []
        );
    }, [currentPath]);
    const {
        loading: namespaceLoading,
        response: namespaces,
        send: refreshNamespace
    } = useApi<'listNamespace'>('listNamespace',undefined,true,true);
    const {send: fetchCluster} = useApi<'listCluster'>('listCluster');
    const [menuItems, setMenuItems] = useState<MenuItem[]>([]);
    useEffect(() => {
        if(!namespaces) {
            return;
        }
        setMenuItems(namespaces.map(ns => ({
            key: `/${ns}`,
            label: ns,
            disabled: true
        })));
        namespaces.forEach(async ns => {
            const clusters = await fetchCluster(ns);
            setMenuItems(oldMenu => {
                const found = oldMenu.find(m => m?.key == `/${ns}`);
                if(!found) {
                    return oldMenu;
                }
                (found as SubMenuType).disabled = false;
                if(Array.isArray(clusters) && clusters.filter(c => c.trim()).length > 0) {
                    (found as SubMenuType).children = clusters.filter(c => c.trim()).map( c => ({
                        key: `/${ns}/${c}`,
                        label: c
                    }));
                }
                return [...oldMenu];
            });
        });
    }, [namespaces]);
    const navigate = useNavigate();
    const [nsCreationModal, setNsCreationModal] = useState(false);
    const [clusterCreationModal, setClusterCreationModal] = useState(false);
    const onNamespaceCreated = useCallback((name: string) => {
        navigate(`/${name}`);
    }, [navigate]);
    const onClusterCreated = useCallback((namespace: string, cluster: string) => {
        navigate(`/${namespace}/${cluster}`);
    }, [navigate]);
    const [openKeys, setOpenKeys] = useState<string[]>([]);
    const [selectedKeys, setSelectedKeys] = useState<string[]>([]);
    return (<div style={{
        display: 'flex',
        flexDirection: 'column',
        height: '100%',
    }}>
        <Space.Compact direction='vertical' style={{margin: '20px 24px'}}>
            <Button onClick={() => setNsCreationModal(true)}>Create Namespace</Button>
            {   
                namespaces &&  namespaces.length
                    ?
                    <Button onClick={() => setClusterCreationModal(true)}>Create Cluster</Button>
                    :
                    null
            }
        </Space.Compact>
        <Divider style={{margin: '0 0 10px'}}/>
        <div className='centered-horizontally-and-vertically-parent' style={{
            flexGrow: 1,
            overflow: 'auto'
        }}>
            {namespaceLoading && <Spin className='centered-horizontally-and-vertically'/>}
            {!namespaceLoading && <Menu
                theme='light'
                mode='inline'
                items={menuItems}
                onSelect={info => navigate(info.key, {state: 'NO_NEED_REFRESH_MENU'})}
                onOpenChange={(openKeys) => setOpenKeys(openKeys)}
                selectedKeys={selectedKeys}
                openKeys={openKeys}
            ></Menu>}
        </div>
        <NamespaceCreationModal 
            open={nsCreationModal} 
            onclose={() => setNsCreationModal(false)}
            oncreated={onNamespaceCreated}
        />
        <ClusterCreationModal 
            open={clusterCreationModal} 
            onclose={() => setClusterCreationModal(false)}
            oncreated={onClusterCreated}
            namespaces={namespaces || []}
        />
    </div>);
}