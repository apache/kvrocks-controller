import { Form, Input, Modal, message } from 'antd';
import { useCallback, useState } from 'react';
import { sendRequest } from '../../hooks/useApi';

export function NamespaceCreationModal(props: {
    open: boolean,
    onclose: () => void,
    oncreated: () => void,
}) {
    const [form] = Form.useForm();
    const [creating, setCreating] = useState(false);
    const onCreate = useCallback(async () => {
        let name = '';
        try {
            const fields = await form.validateFields();
            name = fields.name;
        } catch (error) {
            return;
        }
        setCreating(true);
        const { errorMessage:errFromResponse } = await sendRequest('createNamespace', name);
        setCreating(false);
        if(errFromResponse) {
            message.error({
                content: errFromResponse
            });
            return;
        }
        props.onclose();
        props.oncreated();
    },[]);
    return (<Modal
        title='Namespace Creation'
        open={props.open}
        onCancel={() => props.onclose()}
        onOk={onCreate}
        confirmLoading={creating}
    >
        <Form
            form={form}
        >
            <Form.Item
                name='name'
                label='name'
                rules={[{ 
                    required: true,
                    transform: value => value.trim(),
                    message: 'Please input name of namespace!' 
                }]}
            >
                <Input/>
            </Form.Item>
        </Form>
    </Modal>);
}