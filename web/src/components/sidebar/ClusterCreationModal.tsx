import { Form, Input, Modal } from 'antd';

export function ClusterCreationModal(props: {
    open: boolean,
    onclose: () => void
}) {
    return (<Modal
        title='Cluster Creation'
        open={props.open}
        onCancel={() => props.onclose()}
        onOk={() => props.onclose()}
    >
        <Form>
            <Form.Item label='name'>
                <Input/>
            </Form.Item>
        </Form>
    </Modal>);
}