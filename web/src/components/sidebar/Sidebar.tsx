import { Button, Divider, Menu, MenuProps, Space, Spin } from 'antd';
import { useCallback, useEffect, useState } from 'react';
import { useApi } from '../../hooks/useApi';
import { SubMenuType } from 'antd/es/menu/hooks/useItems';
import { NamespaceCreationModal } from './NamespaceCreationModal';
import { ClusterCreationModal } from './ClusterCreationModal';
import { SelectInfo } from 'rc-menu/lib/interface';
import { useNavigate } from 'react-router-dom';

type MenuItem = Required<MenuProps>['items'][number];

export function Sidebar() {
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
            key: ns,
            label: ns,
            disabled: true
        })));
        namespaces.forEach(async ns => {
            const clusters = await fetchCluster(ns);
            setMenuItems(oldMenu => {
                const found = oldMenu.find(m => m?.key == ns);
                if(!found) {
                    return oldMenu;
                }
                (found as SubMenuType).disabled = false;
                if(Array.isArray(clusters) && clusters.filter(c => c.trim()).length > 0) {
                    (found as SubMenuType).children = clusters.filter(c => c.trim()).map( c => ({
                        key: `${ns}-${c}`,
                        label: c
                    }));
                }
                return [...oldMenu];
            });
        });
    }, [namespaces]);
    const navigate = useNavigate();
    const onMenuSelect = useCallback((info: SelectInfo) => {
        if(info.keyPath.length == 1) {
            selectNamespace(info.key);
        } else if (info.keyPath.length == 2) {
            const ns = info.keyPath[1];
            const cluster = info.keyPath[0].replace(new RegExp(`^${ns}-`), '');
            selectCluster(ns, cluster);
        }
    },[]);
    const selectNamespace = useCallback((namespace: string) => {
        navigate(`/${namespace}`);
    },[]);
    const selectCluster = useCallback((namespace: string, cluster: string) => {
        navigate(`/${namespace}/${cluster}`);
    },[]);
    const [nsCreationModal, setNsCreationModal] = useState(false);
    const [clusterCreationModal, setClusterCreationModal] = useState(false);
    return (<div style={{
        display: 'flex',
        flexDirection: 'column',
        height: '100%',
    }}>
        <Space.Compact direction='vertical' style={{margin: '20px 24px'}}>
            <Button onClick={() => setNsCreationModal(true)}>Create Namespace</Button>
            {   
                namespaces && 
                namespaces.length && 
                <Button onClick={() => setClusterCreationModal(true)}>Create Cluster</Button>
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
                onSelect={onMenuSelect}
            ></Menu>}
        </div>
        <NamespaceCreationModal 
            open={nsCreationModal} 
            onclose={() => setNsCreationModal(false)}
            oncreated={() => refreshNamespace(undefined)}
        />
        <ClusterCreationModal 
            open={clusterCreationModal} 
            onclose={() => setClusterCreationModal(false)}
            oncreated={() => refreshNamespace(undefined)}
            namespaces={namespaces || []}
        />
    </div>);
}