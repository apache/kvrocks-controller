import { DownOutlined } from '@ant-design/icons';
import type { MenuProps } from 'antd';
import {  Dropdown, Space } from 'antd';
const items: MenuProps['items'] = [
    {
        label: '迁移分片',
        key: '1',
    },
    {
        label: '新增节点',
        key: '2',
    },
    {
        type: 'divider',
    },
    {
        label: '删除分片',
        key: '3',
        danger: true,
    },
    {
        label: '删除节点',
        key: '4',
        danger: true,
    },
];

export default function () {

    return (
        <Dropdown menu={{ items }} >
            <a onClick={(e) => e.preventDefault()}>
                <Space>
                    更多
                    <DownOutlined />
                </Space>
            </a>
        </Dropdown>
    )
}