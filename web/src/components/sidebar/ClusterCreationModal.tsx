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
import { Button, Form, Input, InputNumber, Modal, Select } from 'antd';
import { DeleteOutlined, PlusOutlined } from '@ant-design/icons';
import { Cluster } from '../../entitits/Cluster';
import { useApi } from '../../hooks/useApi';
import { useEffect } from 'react';

const labelSpan = 6; // [0,24]
export function ClusterCreationModal(props: {
    open: boolean,
    onclose: () => void,
    oncreated: (namespace: string, cluster: string) => void,
    namespaces: string[],
    defaultNamespace?: string,
    disableNamespaceSelection?: boolean
}) {
    const [form] = Form.useForm();
    useEffect(() => {
        if(props.open && props.defaultNamespace) {
            form.setFieldValue('namespace', props.defaultNamespace);
        }
    }, [props.defaultNamespace, props.open]);
    const {
        loading: creating,
        send: createCluster
    } = useApi('createCluster');
    return (<Modal
        title='Cluster Creation'
        open={props.open}
        onCancel={() => props.onclose()}
        confirmLoading={creating}
        onOk={async () => {
            try {
                await form.validateFields();
            } catch (error) {
                return;
            }
            const clst = new Cluster(form.getFieldsValue());
            const success = await createCluster(clst);
            if(success) {
                props.onclose();
                props.oncreated(clst.namespace, clst.name);
            }
        }}
    >
        <Form
            form={form}
            labelCol={{span: labelSpan}}
            wrapperCol={{span: 24 - labelSpan}}
            autoComplete='off'
        >
            {
                props.namespaces && <Form.Item
                    name="namespace"
                    label="namespace"
                    rules={[{ 
                        required: true,
                        message: 'Please select namespace' 
                    }]}
                    initialValue={props.defaultNamespace}
                >
                    <Select 
                        options={props.namespaces.map(ns => ({
                            value: ns,
                            label: ns
                        }))}
                        disabled={props.disableNamespaceSelection}
                    />
                </Form.Item>
            }
            <Form.Item
                name='name'
                label='cluster name'
                rules={[{
                    required: true,
                    transform: value => typeof value == 'string' ? value.trim() : value,
                    message: 'Please input name of cluster'
                }]}
            >
                <Input autoComplete='off'/>
            </Form.Item>
            <Form.List
                name='nodes'
                rules={[{
                    validator: (rule, value) => new Promise<void>((res, rej) => {
                        if(Array.isArray(value) && value.length > 0) {
                            res();
                        } else {
                            rej(new Error('At least add one node'));
                        }
                    })
                }]}
            >
                {(fields, {add, remove}, {errors}) => (<>
                    {
                        fields.map((field, index) => (
                            <Form.Item
                                {...field}
                                key={field.key}
                                label={index == 0 ? 'nodes' : ''}
                                wrapperCol={index == 0 ? {} : {offset: labelSpan}}
                                rules={[
                                    {
                                        required: true,
                                        message: 'Please input node address',
                                    },
                                ]}
                            >
                                <Input
                                    placeholder='127.0.0.1:6666'
                                    addonAfter={
                                        <DeleteOutlined style={{cursor: 'pointer'}} onClick={() => remove(index)}/>
                                    }
                                />
                            </Form.Item>
                        ))
                    }
                    <Form.Item
                        wrapperCol={fields.length == 0 ? {} : {offset: labelSpan}}
                        label={fields.length == 0 ? 'nodes' : ''}
                    >
                        <Button
                            onClick={() => add()}
                            style={{width: '100%'}}
                            type="dashed"
                        >
                            <PlusOutlined />
                            Add Node
                        </Button>
                        <Form.ErrorList errors={errors}/>
                    </Form.Item>
                </>)}
            </Form.List>
            <Form.Item
                name='password'
                label='password'
            >
                <Input.Password autoComplete='off'/>
            </Form.Item>
            <Form.Item
                name='replicas'
                label='replicas'
            >
                <InputNumber min={0} step={1}/>
            </Form.Item>
        </Form>
    </Modal>);
}