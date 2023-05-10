import { Button, Divider, Menu, MenuProps, Space, Spin } from 'antd';
import { useEffect, useState } from 'react';
import { sendRequest, useApi } from '../../hooks/useApi';
import { SubMenuType } from 'antd/es/menu/hooks/useItems';
import { NamespaceCreationModal } from './NamespaceCreationModal';
import { ClusterCreationModal } from './ClusterCreationModal';

type MenuItem = Required<MenuProps>['items'][number];

export function Sidebar() {
    const [namespaceLoading, namespaces, , refreshNamespace] = useApi<'listNamespace'>('listNamespace', undefined);
    const [menuItems, setMenuItems] = useState<MenuItem[]>([]);
    useEffect(() => {
        if(!namespaces) {
            return;
        }
        setMenuItems(namespaces.map(ns => ({
            key: ns,
            label: ns
        })));
        namespaces.forEach(async ns => {
            const {response: clusters, errorMessage} = await sendRequest('listCluster', ns);
            if(!errorMessage && Array.isArray(clusters) && clusters.filter(c => c).length > 0) {
                setMenuItems(oldMenu => {
                    const found = oldMenu.find(m => m?.key == ns);
                    if(!found) {
                        return oldMenu;
                    }
                    (found as SubMenuType).children = clusters.map( c => ({
                        key: `${ns}-${c}`,
                        label: c
                    }));
                    return [...oldMenu];
                });
            }
        });
    }, [namespaces]);
    const [nsCreationModal, setNsCreationModal] = useState(false);
    const [clusterCreationModal, setClusterCreationModal] = useState(false);
    return (<div style={{
        display: 'flex',
        flexDirection: 'column',
        height: '100%',
    }}>
        <Space.Compact direction='vertical' style={{margin: '20px 24px'}}>
            <Button onClick={() => setNsCreationModal(true)}>Create Namespace</Button>
            <Button onClick={() => setClusterCreationModal(true)}>Create Cluster</Button>
        </Space.Compact>
        <Divider style={{margin: '0 0 10px'}}/>
        <div style={{
            flexGrow: 1,
            overflow: 'auto',
            position: 'relative'
        }}>
            {namespaceLoading && <Spin style={{
                position:'absolute',
                top: '50%',
                left: '50%',
                transform: 'translate(-50%, -50%)',
            }}/>}
            {!namespaceLoading && <Menu
                theme='light'
                mode='inline'
                items={menuItems}
            ></Menu>}
        </div>
        <NamespaceCreationModal 
            open={nsCreationModal} 
            onclose={() => setNsCreationModal(false)}
            oncreated={() => refreshNamespace()}
        />
        <ClusterCreationModal 
            open={clusterCreationModal} 
            onclose={() => setClusterCreationModal(false)}
        />
    </div>);
}