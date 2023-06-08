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
import { Button, Menu, MenuProps, Spin } from 'antd';
import { LeftOutlined } from '@ant-design/icons';
import { CSSProperties, useEffect, useLayoutEffect, useState } from 'react';
import { useApi } from '../../hooks/useApi';
import { useLocation, useNavigate } from 'react-router-dom';

const menuWrapperStyle: CSSProperties = {
    width: '100%',
    flexShrink: 0
};
type MenuItem = Required<MenuProps>['items'][number];

export function Sidebar() {
    const { pathname: currentPath, state: locationState } = useLocation();
    const navigate = useNavigate();
    useEffect(() => {
        if(locationState != 'NO_NEED_REFRESH_MENU') {
            refreshNamespace(undefined);
        }
        const [, namespace, cluster] = currentPath.split('/');
        if(namespace) {
            fetchCluster(namespace);
            setSelectedNamespace(namespace);
            setClusterMode(true);
        }
    }, [currentPath]);

    const [selectedNamespace, setSelectedNamespace] = useState<string>('');
    const [namespaceItems, setNamespaceItems] = useState<MenuItem[]>([]);
    const {
        loading: namespaceLoading,
        response: namespaces,
        send: refreshNamespace
    } = useApi<'listNamespace'>('listNamespace',undefined,true,true);
    useEffect(() => {
        if(!namespaces) {
            return;
        }
        setNamespaceItems(namespaces.map(ns => ({
            key: `/${ns}`,
            label: ns,
        })));
    }, [namespaces]);

    const {
        loading: clusterLoading,
        response: cluster,
        send: fetchCluster
    } = useApi<'listCluster'>('listCluster');
    const [clusterItems, setClusterItems] = useState<MenuItem[]>([]);
    useLayoutEffect(() => {
        if(!cluster) {
            return;
        }
        setClusterItems(cluster.filter(c => c.trim()).map(cst => ({
            key: `/${selectedNamespace}/${cst}`,
            label: cst
        })));
    }, [cluster]);

    const [clusterMode, setClusterMode] = useState(false);

    return (<div style={{
        flexGrow: 1,
        height: '100%',
        width: '100%',
        overflowY: 'auto',
        overflowX: 'clip',
        position: 'relative',
    }}
    >
        <div style={{
            position: 'absolute',
            height: '100%',
            width: '100%',
            left: clusterMode ? '-100%' : '0',
            transition: 'left 300ms',
            display: 'flex'
        }}>
            <div style={menuWrapperStyle} className='centered-horizontally-and-vertically-parent'>
                {namespaceLoading && <Spin className='centered-horizontally-and-vertically'/>}
                {!namespaceLoading && <Menu
                    theme='light'
                    mode='inline'
                    items={namespaceItems}
                    onClick={info => {setClusterMode(true); navigate(info.key, {state: 'NO_NEED_REFRESH_MENU'});}}
                    selectedKeys={selectedNamespace ? [`/${selectedNamespace}`] : []}
                ></Menu>}
            </div>
            <div style={menuWrapperStyle} className='centered-horizontally-and-vertically-parent'>
                <Button style={{marginLeft: '20px'}} onClick={() => setClusterMode(false)}>
                    <LeftOutlined />
                </Button>
                {clusterLoading && <Spin className='centered-horizontally-and-vertically'/>}
                {!clusterLoading && <Menu
                    theme='light'
                    mode='inline'
                    items={clusterItems}
                    onSelect={info => navigate(info.key, {state: 'NO_NEED_REFRESH_MENU'})}
                    selectedKeys={[currentPath]}
                ></Menu>}
            </div>
        </div>
    </div>
    );
}