import { PageContainer } from "@ant-design/pro-components";
import { Space, Flex, Card, Table, Button } from 'antd';

const dataSource = [
    {
        key: '1',
        name: "192.168.1.5",
        age: "从节点异常",
        address: '1 Master + 2 Slave',
    },
    {
        key: '2',
        name: '胡彦祖',
        age: 42,
        address: '西湖区湖底公园1号',
    },
];

const columns = [
    {
        title: 'Master Ip',
        dataIndex: 'name',
        key: 'name',
    },
    {
        title: '状态',
        dataIndex: 'age',
        key: 'age',
    },
    {
        title: '从节点数量',
        dataIndex: 'address',
        key: 'address',
    },
    {
        title: '分片列表',
        dataIndex: 'address',
        key: 'address',
    },
    {
        title: '操作',
        key: 'action',
        render: (_, record) => (
            <Space size="middle">
                <a>查看</a>
                <a>操作</a>
            </Space>
        ),
    },
];

export default function () {
    return (
        <PageContainer title="测试集群">
            <Flex gap="small" vertical>
                <Card style={{ padding: "12px" }}>
                    <Flex gap="middle" align="start" justify="space-around">
                        <Flex gap="small" vertical align="center">
                            <div><strong style={{ "fontSize": "20px" }}>分片数量</strong></div>
                            <div style={{ "fontSize": "20px" }}>3</div>
                        </Flex>
                        <Flex gap="small" vertical align="center">
                            <div><strong style={{ "fontSize": "20px" }}>节点数量</strong></div>
                            <div style={{ "fontSize": "20px" }}>12</div>
                        </Flex>
                    </Flex>
                    <Flex align="start" >
                    </Flex>
                </Card>
                {/* <Card>
                    <Flex gap="small">
                    <Button>新增分片</Button>
                    <Button>删除分片</Button>
                    <Button danger>删除集群</Button>
                    </Flex>
                </Card> */}
                <Card>
                    <Flex>
                        <Button type="primary" style={{ marginBottom: 16 }}>
                            添加分片
                        </Button>
                    </Flex>
                    <Table pagination={false} dataSource={dataSource} columns={columns} />
                </Card>
            </Flex>
        </PageContainer>
    );
}