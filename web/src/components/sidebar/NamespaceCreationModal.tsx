import { Form, Input, Modal } from 'antd';
import { useCallback } from 'react';
import { useApi } from '../../hooks/useApi';

export function NamespaceCreationModal(props: {
    open: boolean,
    onclose: () => void,
    oncreated: (name: string) => void,
}) {
    const [form] = Form.useForm();
    const {
        loading,
        send: createNamespace
    } = useApi('createNamespace');
    const onCreate = useCallback(async () => {
        let name = '';
        try {
            const fields = await form.validateFields();
            name = fields.name;
        } catch (error) {
            return;
        }
        const success = await createNamespace(name);
        if(!success) {
            return;
        }
        props.onclose();
        props.oncreated(name);
    },[]);
    return (<Modal
        title='Namespace Creation'
        open={props.open}
        onCancel={() => props.onclose()}
        onOk={onCreate}
        confirmLoading={loading}
    >
        <Form
            form={form}
        >
            <Form.Item
                name='name'
                label='name'
                rules={[{ 
                    required: true,
                    transform: value => typeof value == 'string' ? value.trim() : value,
                    message: 'Please input name of namespace' 
                }]}
            >
                <Input autoComplete='off'/>
            </Form.Item>
        </Form>
    </Modal>);
}