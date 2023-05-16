import { ConfigProvider, Layout, Space, theme as antdTheme } from 'antd';
import { Logo } from './components/Logo';
import './App.css';
import { Sidebar } from './components/sidebar/Sidebar';
import { useRoutes } from 'react-router-dom';
import { router } from './router';
import { BulbOutlined, GithubOutlined } from '@ant-design/icons';
import { useLocalstorage } from './hooks/useLocalStorage';
import { useCallback } from 'react';


function App() {
    const routerElement = useRoutes(router);
    const [theme, setTheme] = useLocalstorage<'light'|'dark'>('theme', 'light');
    const changeTheme = useCallback(() => {
        setTheme(theme == 'dark' ? 'light' : 'dark');
    },[theme]);
    return (
        <ConfigProvider theme={{algorithm: theme == 'dark' ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm}}>
            <Layout>
                <Layout.Header style={{
                    backgroundColor: theme == 'light' ? 'white' : '',
                    display: 'flex',
                    justifyContent: 'space-between'
                }}>
                    <Logo></Logo>
                    <div style={{fontSize: '20px'}}>
                        <Space size='large'>
                            <BulbOutlined onClick={changeTheme} style={{cursor: 'pointer'}}/>
                            <GithubOutlined style={{cursor: 'pointer'}}/>
                        </Space>
                    </div>
                </Layout.Header>
                <Layout hasSider style={{height: 'calc(100vh - 64px)'}}>
                    <Layout.Sider
                        theme={theme}
                    >
                        <Sidebar/>
                    </Layout.Sider>
                    <Layout.Content style={{padding: '20px', overflow: 'auto'}}>
                        {
                            routerElement
                        }
                    </Layout.Content>
                </Layout>
            </Layout>
        </ConfigProvider>
    );
}

export default App;
