import { Button, Col, Divider, Drawer, Flex, List, Row, Space, Tag, Typography } from "antd";
import { useState } from "react";
const data = [
  '1-20000',
  '44567',
  '2-98656',
];

const ipList = [
  { ip: '192.168.1.1:4000', isMaster: true },
  { ip: '192.168.1.1:4001', isMaster: false },
  { ip: '178.168.2.1:4000', isMaster: false },
];


export default function () {
  const [open, setOpen] = useState(false);
  const showDrawer = () => {
    setOpen(true);
  };
  const onClose = () => {
    setOpen(false);
  };

  return (
    <>
      <a onClick={showDrawer}>查看</a>
      <Drawer 
      title={
        <strong style={{fontSize:"20px"}}>
          分片信息
        </strong>
      }
      width={500} placement="right" closable={false} onClose={onClose} open={open}
      extra={
        <Space>
          <Button type="primary" onClick={onClose}>
            新增节点
          </Button>
        </Space>
      }
      >
        <Flex vertical gap='small'>
          <div>
            <h3>分片范围 </h3>
            <List
              bordered
              dataSource={data}
              renderItem={(item) => (
                <List.Item >
                  <Flex justify="space-between" align="center" style={{ width: "100%", padding: "0 12px" }}>
                    <div>{item}</div>

                    <Button type="text" danger>迁移</Button>
                  </Flex>
                </List.Item>
              )}
            />
          </div>
          <div>
            <Divider />
            <h3 style={{ marginBottom: 24, fontSize: "16px" }}>
              节点列表
            </h3>
            <List
              bordered
              dataSource={ipList}
              renderItem={(item) => (
                <List.Item>
                  <Flex justify="space-between" align="center" style={{ width: "100%", padding: "0 12px" }}>
                    <div>{item.ip}</div>
                    <div style={{ margin: "0 15% 0 0" }}>
                      {item.isMaster ?
                        <Tag color="blue">主节点</Tag> :
                        <Tag>从节点</Tag>
                      }
                    </div>
                    <Button type="text" danger>删除</Button>
                  </Flex>
                </List.Item>
              )}
            />
          </div>
        </Flex>
      </Drawer>
    </>
  );
}