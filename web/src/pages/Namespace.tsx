import { Button, Popconfirm, Space, Typography } from 'antd';
import { useNavigate, useParams } from 'react-router-dom';
import { ClusterCreationModal } from '../components/sidebar/ClusterCreationModal';
import { useCallback, useState } from 'react';
import { useApi } from '../hooks/useApi';

export function Namespace() {
    const {namespace} = useParams();
    const [clusterCreationModal, setClusterCreationModal] = useState(false);
    const navigate = useNavigate();
    const {send: sendDelete} = useApi<'deleteNamespace'>('deleteNamespace');
    const onDelete = useCallback(async () => {
        if(!namespace) {
            return;
        }
        const success = await sendDelete(namespace);
        if(success) {
            navigate('/');
            // todo update menu
        }
    },[namespace]);
    return (<>
        {
            namespace && <div className='centered-horizontally-and-vertically-parent' style={{height: '100%'}}>
                <Typography.Title level={4} style={{margin: 0}}>
                    { `namespace: ${namespace}` }
                </Typography.Title>
                <Space className='centered-horizontally-and-vertically'>
                    <Button size='large' onClick={() => setClusterCreationModal(true)}>Create Cluster</Button>
                    <Popconfirm 
                        title='Delete Namespace'
                        description={<div style={{maxWidth: '50vw', wordBreak: 'break-all'}}>{
                            `Are you sure you want to delete namespace ${namespace}`
                        }</div>}
                        onConfirm={onDelete}
                        okButtonProps={{danger: true}}
                    >
                        <Button size='large' danger>Delete Namespace</Button>
                    </Popconfirm>
                </Space>
                <ClusterCreationModal
                    namespaces={[namespace]}
                    defaultNamespace={namespace}
                    disableNamespaceSelection
                    open={clusterCreationModal} 
                    onclose={() => setClusterCreationModal(false)}
                    oncreated={() => undefined}
                />
            </div>
        }
    </>);
}