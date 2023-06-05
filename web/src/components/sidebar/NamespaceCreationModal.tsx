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