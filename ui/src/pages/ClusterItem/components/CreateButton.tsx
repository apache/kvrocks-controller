import { DownOutlined, PlusOutlined } from '@ant-design/icons';
import type { MenuProps } from 'antd';
import { Button, Dropdown, Space } from 'antd';
const items: MenuProps['items'] = [
    {
        label: '创建分片',
        key: '1',
    },
    {
        label: '创建节点',
        key: '2',
    },
];

const menuProps = {
    items,
    onClick: (item) => {
        console.log(item);
    },
};
export default function () {

    return (
        <Dropdown menu={menuProps} >
            <Button type="primary" style={{ marginBottom: 16 }}>
                <Space>
                    <PlusOutlined />
                    创建
                    <DownOutlined />
                </Space>
            </Button>
        </Dropdown>
    )
}